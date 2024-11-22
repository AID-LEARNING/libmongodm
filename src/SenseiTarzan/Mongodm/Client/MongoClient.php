<?php

namespace SenseiTarzan\Mongodm\Client;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use SenseiTarzan\Mongodm\Class\MongoConfig;
use SenseiTarzan\Mongodm\Class\Request;

class MongoClient
{
	/**
	 * @var Database[]
	 */
	private array	$databases = [];

	/**
	 * @var array<string, Collection>
	 */
	private array		$collections = [];

	private Client		$client;

	public function __construct(MongoConfig $config) {
		$this->client = new Client($config->getUri(), unserialize($config->getUriOptions()), unserialize($config->getDriverOptions()));
		$this->databases['default'] = ($this->databases[strtolower($config->getDbName())] = $this->client->selectDatabase($config->getDbName(), unserialize($config->getDbOptions())));
	}

	/**
	 * @  ;return Client
	 */
	public function getClient(): Client
	{
		return $this->client;
	}

	public function selectCollection(string $collectionName, string $dbName = 'default', array $dbOptions = []): Collection
	{
		return ($this->collections[$collectionName] ?? ($this->collections[$collectionName] = $this->getDatabase($dbName, $dbOptions)->selectCollection($collectionName)));
	}

	/**
	 * @return array
	 */
	public function getCollections(): array
	{
		return $this->collections;
	}

	public function getDatabase(string $dbName = 'default', array $dbOptions = []): ?Database
	{
		return ($this->databases[$dbName] ?? ($this->databases[$dbName] = $this->client->selectDatabase($dbName, $dbOptions)));
	}
}