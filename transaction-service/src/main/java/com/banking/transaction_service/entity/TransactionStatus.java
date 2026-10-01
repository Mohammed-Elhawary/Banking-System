package com.banking.transaction_service.entity;

/*
    Transaction Lifecycle  Flow
    PENDING -> PROCCESSING -> COMPLETED ( CLEAN TRANSACTION )
                           -> PENDING_VERIFICATION (SUSPICIOUS DETECTED)
                                        -> COMPLETED (VIRIFIED)
                                        -> FLAGGED (SAGA REFUND)
                            -> FAILD
                            -> FLAGGED
*/
public enum TransactionStatus {

    PENDING,

    PROCCESSING,

    COMPLETED,

    PENDING_VERIFICATION,

    FLAGGED,

    FAILURE

}
