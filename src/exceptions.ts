
export class ZookeeperLockTimeoutError extends Error {
    lockPath : string;
    timeout? : number;

    constructor(message : string, path : string, timeout? : number) {
        super(message);
        this.message = message;
        this.lockPath = path;
        this.timeout = timeout;
    }
}
