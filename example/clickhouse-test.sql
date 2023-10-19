-- 创建user表，用于读
CREATE TABLE default.user
(
    `id` UInt64,
    `name` String,
    `sex` UInt8,
    `age` UInt8,
    `addr` String,
    `created` DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created)
ORDER BY (created, intHash32(id))
SAMPLE BY intHash32(id);

-- 创建user2表，跟user表一样，用于写
CREATE TABLE default.user2
(
    `id` UInt64,
    `name` String,
    `sex` UInt8,
    `age` UInt8,
    `addr` String,
    `created` DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created)
ORDER BY (created, intHash32(id))
SAMPLE BY intHash32(id);
