<?php

namespace SenseiTarzan\Mongodm\Class;

use pmmp\thread\ThreadSafe;

class MongoConfig extends ThreadSafe
{
	private readonly string $uriOptions;
	private readonly string $driverOptions;
	private readonly string $dbOptions;
	public function __construct(
		private readonly string $uri,
		private readonly string $dbName,
		array $uriOptions = [],
		array $driverOptions = [],
		array $dbOptions = []
	)
	{
		$this->uriOptions = serialize($uriOptions);
		$this->driverOptions = serialize($driverOptions);
		$this->dbOptions = serialize($dbOptions);
	}

	/**
	 * @return string
	 */
	public function getUri(): string
	{
		return $this->uri;
	}

	/**
	 * @return string
	 */
	public function getUriOptions(): string
	{
		return $this->uriOptions;
	}

	/**
	 * @return string
	 */
	public function getDriverOptions(): string
	{
		return $this->driverOptions;
	}

	/**
	 * @return string
	 */
	public function getDbName(): string
	{
		return $this->dbName;
	}

	/**
	 * @return string
	 */
	public function getDbOptions(): string
	{
		return $this->dbOptions;
	}
}