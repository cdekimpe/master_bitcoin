/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe;

/**
 *
 * @author cdekimpe
 */
public class ElasticConfig {
    
    public static final String HOST1 = "storm-supervisor-1";
    public static final String HOST2 = "storm-supervisor-2";
    public static final String HOST3 = "storm-supervisor-3";
    public static final int PORT = 9300;
    public static final String CLUSTER_NAME = "projet3";
    public static final String INDEX = "bitcoin-management";
    
    /* Index mapping
    
    {
    "bitcoin-management": {
        "mappings": {
            "max-value": {
                "properties": {
                    "averageEur": {
                        "type": "float"
                    },
                    "eurValue": {
                        "type": "float"
                    },
                    "maxValue": {
                        "type": "float"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    }
                }
            },
            "rate": {
                "properties": {
                    "eur": {
                        "type": "float"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    }
                }
            },
            "total-volume": {
                "properties": {
                    "averageEur": {
                        "type": "float"
                    },
                    "eurValue": {
                        "type": "float"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    },
                    "totalBitcoin": {
                        "type": "float"
                    }
                }
            },
            "transaction": {
                "properties": {
                    "amount": {
                        "type": "float"
                    },
                    "hash": {
                        "type": "text"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    }
                }
            },
            "block": {
                "properties": {
                    "foundBy": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 64
                            }
                        }
                    },
                    "hash": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 64
                            }
                        }
                    },
                    "reward": {
                        "type": "float"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    }
                }
            },
            "best-miners": {
                "properties": {
                    "averageEur": {
                        "type": "float"
                    },
                    "eurValue": {
                        "type": "float"
                    },
                    "foundBy": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 64
                            }
                        }
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "epoch_second"
                    },
                    "value": {
                        "type": "float"
                    }
                }
            }
        }
    }
}*/
    
}
