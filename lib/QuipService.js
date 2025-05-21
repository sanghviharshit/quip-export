const fetch = require('node-fetch');
const LoggerAdapter = require('./common/LoggerAdapter');
const moment = require('moment');

const TIMES_LIMIT_503 = 10;
const TIMES_LIMIT_429 = 10;

class QuipService {
    constructor(accessToken, apiURL='https://platform.quip.com:443/1') {
        this.accessToken = accessToken;
        this.apiURL = apiURL;
        this.logger = new LoggerAdapter();
        this.querries503 = new Map();
        this.querries429 = new Map();
        this.waitingMs = 1000;
        this.stats = {
            query_count: 0,
            getThread_count: 0,
            getThreads_count: 0,
            getFolder_count: 0,
            getFolders_count: 0,
            getBlob_count: 0,
            getPdf_count: 0,
            getXlsx_count: 0,
            getDocx_count: 0,
            getCurrentUser_count: 0,
            getThreadMessages_count: 0,
            getUser_count: 0
        };
    }

    setLogger(logger) {
        this.logger = logger;
    }

    async checkUser() {
        this.stats.getCurrentUser_count++;

        let attempt = 0;
        while (attempt < TIMES_LIMIT_429) {
            const res = await fetch(`${this.apiURL}/users/current`, this._getOptions('GET'));
            if(res.ok) return true;
            if(res.status === 429) {
                let waitingInMs = this.waitingMs;
                const retryAfter = res.headers.get('retry-after');
                if (retryAfter) {
                    if (!isNaN(retryAfter)) {
                        // retryAfter is seconds
                        waitingInMs = parseInt(retryAfter, 10) * 1000;
                    } else {
                        // retryAfter is a date string
                        const retryDate = new Date(retryAfter);
                        const now = new Date();
                        waitingInMs = retryDate - now;
                    }
                }
                const humanDuration = moment.duration(waitingInMs).humanize();
                this.logger.debug(`User is under rate limit (429). Waiting ${humanDuration} before retrying...`);
                await new Promise(resolve => setTimeout(resolve, waitingInMs));
                attempt++;
                continue;
            }
            return false;
        }
        this.logger.error(`User is under rate limit (429) and max retries reached.`);
        return false;
    }

    async getUser(userIds) {
        this.stats.getUser_count++;
        return this._apiCallJson(`/users/${userIds}`);
    }

    async getCurrentUser() {
        this.stats.getCurrentUser_count++;
        return this._apiCallJson('/users/current');
    }

    async getFolder(folderId) {
        this.stats.getFolder_count++;
        return this._apiCallJson(`/folders/${folderId}`);
    }

    async getThread(threadId) {
        this.stats.getThread_count++;
        return this._apiCallJson(`/threads/${threadId}`);
    }

    async getThreadMessages(threadId) {
        this.stats.getThreadMessages_count++;
        return this._apiCallJson(`/messages/${threadId}`);
    }

    async getThreads(threadIds) {
        this.stats.getThreads_count++;
        return this._apiCallJson(`/threads/?ids=${threadIds}`);
    }

    async getFolders(threadIds) {
        this.stats.getFolders_count++;
        return this._apiCallJson(`/folders/?ids=${threadIds}`);
    }

    async getBlob(threadId, blobId) {
        //const random = (Math.random() > 0.8) ? 'random' : '';
        this.stats.getBlob_count++;
        return this._apiCallBlob(`/blob/${threadId}/${blobId}`);
    }

    async getPdf(threadId) {
        this.stats.getPdf_count++;
        return this._apiCallBlob(`/threads/${threadId}/export/pdf`);
    }

    async getDocx(threadId) {
        this.stats.getDocx_count++;
        return this._apiCallBlob(`/threads/${threadId}/export/docx`);
    }

    async getXlsx(threadId) {
        this.stats.getXlsx_count++;
        return this._apiCallBlob(`/threads/${threadId}/export/xlsx`);
    }

    async _apiCallBlob(url, method = 'GET') {
        return this._apiCall(url, method, true);
    }

    async _apiCallJson(url, method = 'GET') {
        return this._apiCall(url, method, false);
    }

    async _apiCall(url, method, blob) {
        this.stats.query_count++;

        try {
            const res = await fetch(`${this.apiURL}${url}`, this._getOptions(method));
            if(!res.ok) {
                if(res.status === 503) {
                    const currentTime = new Date().getTime();
                    const rateLimitReset = +res.headers.get('x-ratelimit-reset')*1000;
                    let waitingInMs = this.waitingMs;
                    if(rateLimitReset > currentTime) {
                        waitingInMs = rateLimitReset - currentTime;
                    }
                    this.logger.debug(`HTTP 503: for ${url}, waiting in ms: ${waitingInMs}`);
                    if(this._check503Query(url)) {
                        return new Promise(resolve => setTimeout(() => {
                            resolve(this._apiCall(url, method, blob));
                        }, waitingInMs));
                    } else {
                        this.logger.error(`Couldn't fetch ${url}, tryed to get it ${TIMES_LIMIT_503} times`);
                        return;
                    }
                } else if(res.status === 429) {
                    let waitingInMs = this.waitingMs;
                    const retryAfter = res.headers.get('retry-after');
                    if (retryAfter) {
                        if (!isNaN(retryAfter)) {
                            // retryAfter is seconds
                            waitingInMs = parseInt(retryAfter, 10) * 1000;
                        } else {
                            // retryAfter is a date string
                            const retryDate = new Date(retryAfter);
                            const now = new Date();
                            waitingInMs = retryDate - now;
                        }
                    }
                    const humanDuration = moment.duration(waitingInMs).humanize();
                    this.logger.debug(`User is under rate limit (429). Waiting ${humanDuration} before retrying...`);
                    if(this._check429Query(url)) {
                        return new Promise(resolve => setTimeout(() => {
                            resolve(this._apiCall(url, method, blob));
                        }, waitingInMs));
                    } else {
                        this.logger.error(`Couldn't fetch ${url}, tryed to get it ${TIMES_LIMIT_429} times (rate limited)`);
                        return;
                    }
                } else {
                    this.logger.debug(`Couldn't fetch ${url}, received ${res.status}`);
                    return;
                }
            }

            if(blob) {
                return res.blob();
            } else {
                return res.json();
            }
        } catch (e) {
            this.logger.error(`Couldn't fetch ${url}, `, e);
        }
    }

    _check503Query(url) {
        let count = this.querries503.get(url);
        if(!count) {
            count = 0;
        }

        this.querries503.set(url, ++count);
        if(count > TIMES_LIMIT_503) {
            return false;
        }

        return true;
    }

    _check429Query(url) {
        let count = this.querries429.get(url);
        if(!count) {
            count = 0;
        }

        this.querries429.set(url, ++count);
        if(count > TIMES_LIMIT_429) {
            return false;
        }

        return true;
    }

    _getOptions(method) {
        return {
            method: method,
            headers: {
                'Authorization': 'Bearer ' + this.accessToken,
                'Content-Type': 'application/json'
            }
        };
    }
}

module.exports = QuipService;