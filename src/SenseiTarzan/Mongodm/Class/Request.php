<?php

namespace SenseiTarzan\Mongodm\Class;

use MongoDB\Collection;
use MongoDB\Database;
use pmmp\thread\ThreadSafe;
use SenseiTarzan\Mongodm\Client\MongoClient;
use Throwable;

abstract class Request
{
	public const COLLECTION_NAME = null;

	/**
	 * @param MongoClient $client
	 * @param array $argv
	 * @return Response
	 */
	public static function run(MongoClient $client, array $argv): Response{
		try {
			return static::request($client->selectCollection(static::COLLECTION_NAME), $argv);
		} catch (Throwable $th) {
			throw new MongoError(MongoError::STAGE_EXECUTE, $th->getMessage());
		}
	}
	protected static function request(Collection $collection, array $argv): Response{
		return Response::getEmpty();
	}
}