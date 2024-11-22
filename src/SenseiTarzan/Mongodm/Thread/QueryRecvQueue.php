<?php

namespace SenseiTarzan\Mongodm\Thread;

use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;
use SenseiTarzan\Mongodm\Class\MongoError;
use SenseiTarzan\Mongodm\Class\Response;

class QueryRecvQueue extends ThreadSafe
{
	private int $availableThreads = 0;

	private ThreadSafeArray $queue;

	public function __construct(){
		$this->queue = new ThreadSafeArray();
	}

	/**
	 * @param int $queryId
	 * @param Response $result
	 */
	public function publishResult(int $queryId, Response $result) : void{
		$this->synchronized(function() use ($queryId, $result) : void{
			$this->queue[] = igbinary_serialize([$queryId, $result]);
			$this->notify();
		});
	}

	public function publishError(int $queryId, MongoError $error) : void{
		$this->synchronized(function() use ($error, $queryId) : void{
			$this->queue[] = igbinary_serialize([$queryId, $error]);
			$this->notify();
		});
	}

	public function fetchResults(&$queryId, &$results) : bool{
		$row = $this->queue->shift();
		if(is_string($row)){
			[$queryId, $results] = igbinary_unserialize($row);
			return true;
		}
		return false;
	}

	/**
	 * @return array
	 */
	public function fetchAllResults(): array{
		return $this->synchronized(function(): array{
			$ret = [];
			while($this->fetchResults($queryId, $results)){
				$ret[] = [$queryId, $results];
			}
			return $ret;
		});
	}

	/**
	 * @return list<array{int, MongoError|Response|null}>
	 */
	public function waitForResults(int $expectedResults): array{
		return $this->synchronized(function() use ($expectedResults) : array{
			$ret = [];
			while(count($ret) < $expectedResults){
				if(!$this->fetchResults($queryId, $results)){
					$this->wait();
					continue;
				}
				$ret[] = [$queryId, $results];
			}
			return $ret;
		});
	}
}