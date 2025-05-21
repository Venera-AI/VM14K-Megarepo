CREATE TABLE "etls"."fConnConfigs" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "nameLabel" VARCHAR(128) NOT NULL,
  "nameValue" VARCHAR(128) UNIQUE NOT NULL,
  "type" VARCHAR(32) NOT NULL,
  "config" JSON NOT NULL,
  UNIQUE ("nameValue")
);

CREATE TYPE "etls"."fConnConfigsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "nameLabel" VARCHAR(128),
  "nameValue" VARCHAR(128),
  "type" VARCHAR(32),
  "config" JSON
);

CREATE TABLE "etls"."fExtConfigs" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "nameLabel" VARCHAR(128) NOT NULL,
  "nameValue" VARCHAR(128) UNIQUE NOT NULL,
  "type" VARCHAR(32) NOT NULL,
  "config" JSON NOT NULL,
  UNIQUE ("nameValue")
);

CREATE TYPE "etls"."fExtConfigsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "nameLabel" VARCHAR(128),
  "nameValue" VARCHAR(128),
  "type" VARCHAR(32),
  "config" JSON
);

CREATE TABLE "etls"."fLoaderConfigs" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "nameLabel" VARCHAR(128) UNIQUE NOT NULL,
  "nameValue" VARCHAR(128) NOT NULL,
  "type" VARCHAR(32) NOT NULL,
  "config" JSON NOT NULL,
  UNIQUE ("nameValue")
);

CREATE TYPE "etls"."fLoaderConfigsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "nameLabel" VARCHAR(128),
  "nameValue" VARCHAR(128),
  "type" VARCHAR(32),
  "config" JSON
);

CREATE TABLE "etls"."fPipelineConfigs" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "nameLabel" VARCHAR(128) NOT NULL,
  "nameValue" VARCHAR(128) NOT NULL UNIQUE,
  "fExtConfigNameValue" VARCHAR(128) NOT NULL,
  "fLoaderConfigNameValue" VARCHAR(128) NOT NULL,
  "transferMode" VARCHAR(128) NOT NULL,
  "transformSql" TEXT,
  "privateFieldPrefix" VARCHAR(128),
  "includePrivateFields" JSON,
  "tagNameValue" VARCHAR(128),
  UNIQUE ("fExtConfigNameValue", "fLoaderConfigNameValue"),
  FOREIGN KEY ("fExtConfigNameValue") REFERENCES "etls"."fExtConfigs" ("nameValue") ON DELETE RESTRICT,
  FOREIGN KEY ("fLoaderConfigNameValue") REFERENCES "etls"."fLoaderConfigs" ("nameValue") ON DELETE RESTRICT
);

CREATE TYPE "etls"."fPipelineConfigsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "nameLabel" VARCHAR(128),
  "nameValue" VARCHAR(128),
  "fExtConfigNameValue" VARCHAR(128),
  "fLoaderConfigNameValue" VARCHAR(128),
  "transferMode" VARCHAR(128),
  "transformSql" TEXT,
  "privateFieldPrefix" VARCHAR(128),
  "includePrivateFields" JSON,
  "tagNameValue" VARCHAR(128)
);

CREATE TABLE "etls"."fExtConnDeps" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "fExtgConfigNameValue" VARCHAR(128) NOT NULL,
  "fConnConfigNameValue" VARCHAR(128) NOT NULL,
  "connArgName" VARCHAR(64) NOT NULL,
  FOREIGN KEY ("fExtgConfigNameValue") REFERENCES "etls"."fExtConfigs" ("nameValue") ON DELETE RESTRICT,
  FOREIGN KEY ("fConnConfigNameValue") REFERENCES "etls"."fConnConfigs" ("nameValue") ON DELETE RESTRICT
);

CREATE TYPE "etls"."fExtConnDepsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "fExtgConfigNameValue" VARCHAR(128),
  "fConnConfigNameValue" VARCHAR(128),
  "connArgName" VARCHAR(64)
);

CREATE TABLE "etls"."fLoaderConnDeps" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "fLoaderConfigNameValue" VARCHAR(128) NOT NULL,
  "fConnConfigNameValue" VARCHAR(128) NOT NULL,
  "connArgName" VARCHAR(64) NOT NULL,
  FOREIGN KEY ("fLoaderConfigNameValue") REFERENCES "etls"."fLoaderConfigs" ("nameValue") ON DELETE RESTRICT,
  FOREIGN KEY ("fConnConfigNameValue") REFERENCES "etls"."fConnConfigs" ("nameValue") ON DELETE RESTRICT
);

CREATE TYPE "etls"."fLoaderConnDepsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "fLoaderConfigNameValue" VARCHAR(128),
  "fConnConfigNameValue" VARCHAR(128),
  "connArgName" VARCHAR(64)
);

CREATE TABLE "etls"."fSparkTransformerConfigs" (
  "createdAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "updatedAt" TIMESTAMP DEFAULT NOW () NOT NULL,
  "id" SERIAL PRIMARY KEY,
  "nameLabel" VARCHAR(128) NOT NULL,
  "nameValue" VARCHAR(128) UNIQUE NOT NULL,
  "storageFConnConfigNameValue" VARCHAR(128) NOT NULL,
  "srcViewConfig" JSON NOT NULL,
  "srcFolder" VARCHAR(1024) NOT NULL,
  "dstFolder" VARCHAR(1024) NOT NULL,
  "transformSql" TEXT,
  UNIQUE ("nameValue"),
  FOREIGN KEY ("storageFConnConfigNameValue") REFERENCES "etls"."fConnConfigs" ("nameValue") ON DELETE RESTRICT
);

CREATE TYPE "etls"."fSparkTransformerConfigsType" AS (
  "createdAt" TIMESTAMP,
  "updatedAt" TIMESTAMP,
  "id" INT,
  "nameLabel" VARCHAR(128),
  "nameValue" VARCHAR(128),
  "storageFConnConfigNameValue" VARCHAR(128),
  "srcViewConfig" JSON,
  "srcFolder" VARCHAR(1024),
  "dstFolder" VARCHAR(1024),
  "transformSql" TEXT
);
