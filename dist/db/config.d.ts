export interface SQLQuery {
    text: string;
    values: any[];
    name?: string;
}
export interface Config {
    transactionAttemptsMax: number;
    transactionRetryDelay: {
        minMs: number;
        maxMs: number;
    };
    castArrayParamsToJson: boolean;
    castObjectParamsToJson: boolean;
    queryListener?(query: SQLQuery, txnId?: number): void;
    resultListener?(result: any, txnId?: number, elapsedMs?: number, query?: SQLQuery): void;
    transactionListener?(message: string, txnId?: number): void;
}
export type NewConfig = Partial<Config>;
/**
 * Get (a copy of) the current configuration.
 */
export declare const getConfig: () => {
    transactionAttemptsMax: number;
    transactionRetryDelay: {
        minMs: number;
        maxMs: number;
    };
    castArrayParamsToJson: boolean;
    castObjectParamsToJson: boolean;
    queryListener?(query: SQLQuery, txnId?: number): void;
    resultListener?(result: any, txnId?: number, elapsedMs?: number, query?: SQLQuery): void;
    transactionListener?(message: string, txnId?: number): void;
};
/**
 * Set key(s) on the configuration.
 * @param newConfig Partial configuration object
 */
export declare const setConfig: (newConfig: NewConfig) => {
    transactionAttemptsMax: number;
    transactionRetryDelay: {
        minMs: number;
        maxMs: number;
    };
    castArrayParamsToJson: boolean;
    castObjectParamsToJson: boolean;
    queryListener?: (query: SQLQuery, txnId?: number) => void;
    resultListener?: (result: any, txnId?: number, elapsedMs?: number, query?: SQLQuery) => void;
    transactionListener?: (message: string, txnId?: number) => void;
};
