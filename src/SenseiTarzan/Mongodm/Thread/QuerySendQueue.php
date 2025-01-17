<?php

namespace SenseiTarzan\Mongodm\Thread;

use Closure;
use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;
use SenseiTarzan\Mongodm\Class\ETypeRequest;
use SenseiTarzan\Mongodm\Class\Request;
use SenseiTarzan\Mongodm\Exception\QueueShutdownException;

class QuerySendQueue extends ThreadSafe
{
	/** @var bool */
	private bool $invalidated = false;
	/** @var ThreadSafeArray */
	private ThreadSafeArray $queries;

	public function __construct(){
		$this->queries = new ThreadSafeArray();
	}

    /**
     * @param int $queryId
     * @param ETypeRequest $type
     * @param string|Closure $request
     * @param array|string|null $argv
     * @return void
     * @throws QueueShutdownException
     */
	public function scheduleQuery(int $queryId, ETypeRequest $type, string|Closure $request, array|string|null $argv = null): void {
		if($this->invalidated){
			throw new QueueShutdownException("You cannot schedule a query on an invalidated queue.");
		}
		$this->synchronized(function() use ($queryId, $type, $request, $argv) : void{
            $this->queries[] = ThreadSafeArray::fromArray([$queryId, $type->value, $request, igbinary_serialize($argv)]);
			$this->notifyOne();
		});
	}

	public function fetchQuery() : ?string {
		return $this->synchronized(function(): ?string {
			while($this->queries->count() === 0 && !$this->isInvalidated()){
				$this->wait();
			}
			return $this->queries->shift();
		});
	}

	public function invalidate() : void {
		$this->synchronized(function():void{
			$this->invalidated = true;
			$this->notify();
		});
	}

	/**
	 * @return bool
	 */
	public function isInvalidated(): bool {
		return $this->invalidated;
	}

	public function count() : int{
		return $this->queries->count();
	}
}