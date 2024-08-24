from django.conf import settings
from pymongo import MongoClient


class MongoDBHandler:
    def __init__(self):
        # Connect to MongoDB using settings
        self.client = MongoClient(settings.MONGODB_URI)
        self.db = self.client[settings.MONGODB_DB_NAME]

    def get_total_sales_over_time(self, interval="daily"):
        collection = self.db["shopifyOrders"]

        pipeline = [
            {
                "$addFields": {
                    "created_at": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$created_at"}, "string"]},
                            "then": {"$dateFromString": {"dateString": "$created_at"}},
                            "else": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": "$created_at"}, "int"]},
                                    "then": {"$toDate": "$created_at"},
                                    "else": "$created_at",
                                }
                            },
                        }
                    }
                }
            }
        ]

        if interval == "daily":
            pipeline.extend(
                [
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                }
                            },
                            "total_sales": {
                                "$sum": "$total_price_set.shop_money.amount"
                            },
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        elif interval == "monthly":
            pipeline.extend(
                [
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m",
                                    "date": "$created_at",
                                }
                            },
                            "total_sales": {
                                "$sum": "$total_price_set.shop_money.amount"
                            },
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        elif interval == "yearly":
            pipeline.extend(
                [
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {"format": "%Y", "date": "$created_at"}
                            },
                            "total_sales": {
                                "$sum": "$total_price_set.shop_money.amount"
                            },
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )

        sales_data = list(collection.aggregate(pipeline))
        return sales_data

    def get_sales_growth_rate(self, interval="daily"):
        total_sales = self.get_total_sales_over_time(interval)
        growth_rate = []

        for i in range(1, len(total_sales)):
            previous = total_sales[i - 1]["total_sales"]
            current = total_sales[i]["total_sales"]
            growth = ((current - previous) / previous) * 100 if previous != 0 else 0
            growth_rate.append({"_id": total_sales[i]["_id"], "growth_rate": growth})

        return growth_rate

    def get_new_customers_over_time(self, interval="daily"):
        collection = self.db["shopifyCustomers"]

        try:
            pipeline = []
            if interval == "daily":
                pipeline = [
                    {
                        "$addFields": {
                            "created_at": {
                                "$convert": {
                                    "input": "$created_at",
                                    "to": "date",
                                    "onError": None,  # Set to None or a default date if conversion fails
                                    "onNull": None,  # Set to None if field is null
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "created_at": {
                                "$ne": None
                            }  # Filter out documents where created_at could not be converted
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                }
                            },
                            "new_customers": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            elif interval == "monthly":
                pipeline = [
                    {
                        "$addFields": {
                            "created_at": {
                                "$convert": {
                                    "input": "$created_at",
                                    "to": "date",
                                    "onError": None,
                                    "onNull": None,
                                }
                            }
                        }
                    },
                    {"$match": {"created_at": {"$ne": None}}},
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m",
                                    "date": "$created_at",
                                }
                            },
                            "new_customers": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            elif interval == "yearly":
                pipeline = [
                    {
                        "$addFields": {
                            "created_at": {
                                "$convert": {
                                    "input": "$created_at",
                                    "to": "date",
                                    "onError": None,
                                    "onNull": None,
                                }
                            }
                        }
                    },
                    {"$match": {"created_at": {"$ne": None}}},
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {"format": "%Y", "date": "$created_at"}
                            },
                            "new_customers": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            else:
                raise ValueError("Invalid interval")

            customer_data = list(collection.aggregate(pipeline))
            return customer_data

        except Exception as e:
            print(f"Error in get_new_customers_over_time: {str(e)}")
            raise

    def get_repeat_customers_over_time(self, interval="daily"):
        collection = self.db["shopifyOrders"]

        pipeline = [
            {
                "$addFields": {
                    "created_at": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$created_at"}, "string"]},
                            "then": {"$dateFromString": {"dateString": "$created_at"}},
                            "else": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": "$created_at"}, "int"]},
                                    "then": {"$toDate": "$created_at"},
                                    "else": "$created_at",
                                }
                            },
                        }
                    }
                }
            }
        ]

        if interval == "daily":
            date_format = "%Y-%m-%d"
        elif interval == "monthly":
            date_format = "%Y-%m"
        elif interval == "quarterly":
            date_format = "%Y-Q"
        elif interval == "yearly":
            date_format = "%Y"
        else:
            raise ValueError("Invalid interval")

        pipeline.extend(
            [
                {
                    "$group": {
                        "_id": {
                            "date": {
                                "$dateToString": {
                                    "format": date_format,
                                    "date": "$created_at",
                                }
                            },
                            "customer_id": "$customer.id",
                        },
                        "purchase_count": {"$sum": 1},
                    }
                },
                {"$match": {"purchase_count": {"$gt": 1}}},
                {
                    "$group": {
                        "_id": "$_id.date",
                        "repeat_customers_count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )

        repeat_customers_data = list(collection.aggregate(pipeline))
        return repeat_customers_data

    def get_geographical_distribution(self):
        collection = self.db["shopifyOrders"]

        pipeline = [
            {
                "$group": {
                    "_id": "$customer.default_address.city",
                    "customer_count": {"$sum": 1},
                }
            },
            {"$sort": {"customer_count": -1}},
        ]

        geographical_distribution = list(collection.aggregate(pipeline))
        return geographical_distribution

    def get_customer_lifetime_value(self):
        collection = self.db["shopifyOrders"]

        pipeline = [
            {
                "$group": {
                    "_id": "$customer.id",  # Group by customer ID
                    "first_purchase_date": {"$min": "$created_at"},
                    "lifetime_value": {"$sum": "$total_price_set.shop_money.amount"},
                }
            },
            {
                "$addFields": {
                    "first_purchase_month": {
                        "$dateToString": {
                            "format": "%Y-%m",
                            "date": {
                                "$dateFromString": {
                                    "dateString": "$first_purchase_date"
                                }
                            },
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$first_purchase_month",
                    "average_lifetime_value": {"$avg": "$lifetime_value"},
                    "customer_count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]

        try:
            clv_data = list(collection.aggregate(pipeline))
            return clv_data
        except Exception as e:
            print(f"Error in get_customer_lifetime_value: {str(e)}")
            raise
