

declare module locators {
    interface Location {
        host: string;
        port?: number;
    }

    interface Locator {
        (): Q.Promise<Location>;

        // Event emitter extension
        addListener?(event: string, listener: Function): any;
        on?(event: string, listener: Function): any;
        once?(event: string, listener: Function): any;
        removeListener?(event: string, listener: Function): any;
        removeAllListeners?(event?: string): any;
        setMaxListeners?(n: number): void;
        listeners?(event: string): Function[];
        emit?(event: string, ...args: any[]): boolean;
    }

    function simple(parameters? : any) : (location : any) =>  Locator;
    function request(parameters? : any) : (location : any) => Locator;
    function zookeeper(parameters? : any) : (location : any) => Locator;
    var EXCEPTION_CODE : any;
}

declare module "locators" {
    export = locators;
}