import time
import pymongo

mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
mongo = mongo_client["ecommerce"]

def mongo_aggregation():
    t = time.perf_counter()
    print("Starting mongo aggregation...")
    results = list(mongo.orders.aggregate([
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products", "localField": "items.product_id",
            "foreignField": "id", "as": "product"
        }},
        {"$unwind": "$product"},
        {"$group": {
            "_id":     "$product.category",
            "revenue": {"$sum": {"$multiply": ["$items.quantity", "$items.unit_price"]}},
            "count":   {"$sum": 1}
        }},
        {"$sort": {"revenue": -1}}
    ]))
    elapsed = time.perf_counter() - t
    print(f"Aggregation took {elapsed:.3f}s. Result count: {len(results)}")
    return elapsed

if __name__ == "__main__":
    mongo_aggregation()
