from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the DataFrame
schema = StructType(
        [
            StructField("xn:SubNetwork._id", StringType(), True),
            StructField(
                "xn:SubNetwork.xn:MeContext",
                ArrayType(
                    StructType(
                        [
                            StructField("_id", StringType(), True),
                            StructField(
                                "xn:ManagedElement",
                                StructType(
                                    [
                                        StructField("_id", StringType(), True),
                                        StructField(
                                            "xn:VsDataContainer",
                                            StructType(
                                                [
                                                    StructField("_id", StringType(), True),
                                                    StructField(
                                                        "xn:attributes",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "es:vsDataSystemFunctions",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "xn:vsDataFormatVersion",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "xn:vsDataType",
                                                                    StringType(),
                                                                    True,
                                                                ),
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "xn:VsDataContainer",
                                                        ArrayType(
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "_id",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "xn:attributes",
                                                                        StructType(
                                                                            [
                                                                                StructField(
                                                                                    "es:vsDataSwInventory",
                                                                                    StructType(
                                                                                        [
                                                                                            StructField(
                                                                                                "es:vsDataSwItem",
                                                                                                StructType(
                                                                                                    [
                                                                                                        StructField(
                                                                                                            "es:swItemId",
                                                                                                            StringType(),
                                                                                                            True,
                                                                                                        ),
                                                                                                        StructField(
                                                                                                            "es:additionalInfo",
                                                                                                            StringType(),
                                                                                                            True,
                                                                                                        ),
                                                                                                        StructField(
                                                                                                            "es:administrativeData",
                                                                                                            StructType(
                                                                                                                [
                                                                                                                    StructField(
                                                                                                                        "es:type",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                    StructField(
                                                                                                                        "es:description",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                    StructField(
                                                                                                                        "es:productName",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                    StructField(
                                                                                                                        "es:productNumber",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                    StructField(
                                                                                                                        "es:productRevision",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                    StructField(
                                                                                                                        "es:productionDate",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    ),
                                                                                                                ]
                                                                                                            ),
                                                                                                            True,
                                                                                                        ),
                                                                                                    ]
                                                                                                ),
                                                                                                True,
                                                                                            )
                                                                                        ]
                                                                                    ),
                                                                                    True,
                                                                                ),
                                                                                StructField(
                                                                                    "xn:vsDataFormatVersion",
                                                                                    StringType(),
                                                                                    True,
                                                                                ),
                                                                                StructField(
                                                                                    "xn:vsDataType",
                                                                                    StringType(),
                                                                                    True,
                                                                                ),
                                                                            ]
                                                                        ),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "xn:VsDataContainer",
                                                                        ArrayType(
                                                                            StructType(
                                                                                [
                                                                                    StructField(
                                                                                        "_id",
                                                                                        StringType(),
                                                                                        True,
                                                                                    ),
                                                                                    StructField(
                                                                                        "xn:attributes",
                                                                                        StructType(
                                                                                            [
                                                                                                StructField(
                                                                                                    "xn:vsDataFormatVersion",
                                                                                                    StringType(),
                                                                                                    True,
                                                                                                ),
                                                                                                StructField(
                                                                                                    "xn:vsDataType",
                                                                                                    StringType(),
                                                                                                    True,
                                                                                                ),
                                                                                            ]
                                                                                        ),
                                                                                        True,
                                                                                    ),
                                                                                ]
                                                                            )
                                                                        ),
                                                                        True,
                                                                    ),
                                                                ]
                                                            )
                                                        ),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

# Create a sample DataFrame with data matching the schema
data = [
        (
            "SubNetwork=ONRM_ROOT_MO,SubNetwork=VRF_Public_MPLS,SubNetwork=MPLS_TNL_SN",
            [
                {
                    "_id": "MeContext=SN_MPLS_TNL_0001",
                    "xn:ManagedElement": {
                        "_id": "ManagedElement=389420011",
                        "xn:VsDataContainer": {
                            "_id": "VsDataContainer=1",
                            "xn:attributes": {
                                "es:vsDataSystemFunctions": "MME, PGW, SGW",
                                "xn:vsDataFormatVersion": "1.0",
                                "xn:vsDataType": "vsDataNeSw",
                            },
                            "xn:VsDataContainer": [
                                {
                                    "_id": "VsDataContainer=2",
                                    "xn:attributes": {
                                        "es:vsDataSwInventory": {
                                            "es:vsDataSwItem": {
                                                "es:swItemId": "1",
                                                "es:additionalInfo": "Additional Info 1",
                                                "es:administrativeData": {
                                                    "es:type": "Software type 1",
                                                    "es:description": "Software description 1",
                                                    "es:productName": "Product name 1",
                                                    "es:productNumber": "12345",
                                                    "es:productRevision": "1.1",
                                                    "es:productionDate": "2024-10-19",
                                                },
                                            }
                                        },
                                        "xn:vsDataFormatVersion": "1.1",
                                        "xn:vsDataType": "vsDataSwItm",
                                    },
                                    "xn:VsDataContainer": [
                                        {
                                            "_id": "VsDataContainer=3",
                                            "xn:attributes": {
                                                "xn:vsDataFormatVersion": "1.2",
                                                "xn:vsDataType": "vsDataSwVer",
                                            },
                                        }
                                    ],
                                }
                            ],
                        },
                    },
                }
            ],
        )
    ]