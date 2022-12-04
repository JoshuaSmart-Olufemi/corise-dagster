from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String}, 
    required_resource_keys={"s3"},
    tags = {"kind": "s3"})
def get_s3_data(context) -> List[Stock]:

    s3_key = context.op_config.get('s3_key')
    data = context.resources.s3.get_data(s3_key)
    return[Stock.from_list(row) for row in data]
 

@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda s: s.high, reverse=True)
    max_stock = sorted_stocks[0]
    return Aggregation(date=max_stock.date, high=max_stock.high)
    

@op(
    required_resource_keys={"redis"},
    tags = {"kind": "redis"} 
    )
def put_redis_data(context, max_stock: Aggregation) -> Nothing:
    context.resources.redis.put_data(str(max_stock.date), str(max_stock.high))


@op(
    required_resource_keys={"s3"},
    tags = {"kind": "s3"} 
    )
def put_s3_data(context, max_stock: Aggregation) -> Nothing:
    s3_key = str(max_stock.date)
    context.resources.s3.put_data(s3_key, max_stock)


@graph
def week_2_pipeline():
    data = get_s3_data()
    processed_data = process_data(data)
    put_redis_data(processed_data)
    put_s3_data(processed_data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource,"redis": redis_resource}

)
