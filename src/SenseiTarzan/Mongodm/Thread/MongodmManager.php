<?php

namespace SenseiTarzan\Mongodm\Thread;

use Error;
use Exception;
use pocketmine\plugin\Plugin;
use pocketmine\snooze\SleeperHandlerEntry;
use ReflectionClass;
use SenseiTarzan\Mongodm\Class\MongoConfig;
use SenseiTarzan\Mongodm\Class\MongoError;
use SenseiTarzan\Mongodm\Class\Request;
use SenseiTarzan\Mongodm\Class\Response;
use SenseiTarzan\Mongodm\Exception\QueueShutdownException;
use SenseiTarzan\Mongodm\libmongodm;
use SplFixedArray;
use pocketmine\Server;
use pocketmine\utils\Terminal;

class MongodmManager
{
	private SleeperHandlerEntry $sleeperHandlerEntry;
	/**
	 * @var SplFixedArray<ThreadMongodm>
	 */
	private SplFixedArray $workers;
	private QuerySendQueue $bufferSend;
	private QueryRecvQueue $bufferRecv;

	private array $handlers = [];
	private int $workerCount = 0;
	private static int $queryId = 0;
	private readonly \AttachableLogger $logger;

	public function __construct(
		Plugin $plugin,
		private string $vendors,
		int $workerCount,
		private readonly MongoConfig $config
	)
	{
		$this->logger = $plugin->getLogger();
		$this->workers = new SplFixedArray($workerCount);
		$this->bufferSend = new QuerySendQueue();
		$this->bufferRecv = new QueryRecvQueue();
		$this->sleeperHandlerEntry = Server::getInstance()->getTickSleeper()->addNotifier(function(): void {
			$this->checkResults();
		});
		$this->addWorker();
	}

	public function addWorker(): void
	{
		$this->workers[$this->workerCount++] =  new ThreadMongodm($this->sleeperHandlerEntry, $this->vendors, $this->bufferSend, $this->bufferRecv, $this->config);
	}

	public function stopRunning(): void
	{
		//$this->bufferSend->invalidate();
		/** @var ThreadMongodm[] $worker */
		$iterator = $this->workers->getIterator();
		while ($iterator->valid()) {
			$worker = $iterator->current();
			$worker?->stopRunning();
			$iterator->next();
		}
	}

	public function quit(): void
	{
		$this->stopRunning();
	}

	/**
	 * @param class-string<Request> $request
	 * @param array $argv
	 * @param callable|null $handler
	 * @param callable|null $onError
	 * @throws QueueShutdownException
	 */
	public function executeRequest(string $request, array $argv = [], ?callable $handler = null, ?callable $onError = null) : void{
		$queryId = self::$queryId++;
		$trace = libmongodm::isPackaged() ? null : new Exception("(This is the original stack trace for the following error)");
		$this->handlers[$queryId] = function(MongoError|Response $results) use ($handler, $onError, $trace){
			if($results instanceof MongoError){
				$this->reportError($onError, $results, $trace);
			}else{
				if ($handler === null)
					return ;
				try{
					$handler($results);
				}catch(Exception $e){
					if(!libmongodm::isPackaged()){
						$prop = (new ReflectionClass(Exception::class))->getProperty("trace");
						$newTrace = $prop->getValue($e);
						$oldTrace = $prop->getValue($trace);
						for($i = count($newTrace) - 1, $j = count($oldTrace) - 1; $i >= 0 && $j >= 0 && $newTrace[$i] === $oldTrace[$j]; --$i, --$j){
							array_pop($newTrace);
						}
						$prop->setValue($e, array_merge($newTrace, [
							[
								"function" => Terminal::$COLOR_YELLOW . "--- below is the original stack trace ---" . Terminal::$FORMAT_RESET,
							],
						], $oldTrace));
					}
					throw $e;
				}catch(Error $e){
					if(!libmongodm::isPackaged()){
						$exceptionProperty = (new ReflectionClass(Exception::class))->getProperty("trace");
						$oldTrace = $exceptionProperty->getValue($trace);

						$errorProperty = (new ReflectionClass(Error::class))->getProperty("trace");
						$newTrace = $errorProperty->getValue($e);

						for($i = count($newTrace) - 1, $j = count($oldTrace) - 1; $i >= 0 && $j >= 0 && $newTrace[$i] === $oldTrace[$j]; --$i, --$j){
							array_pop($newTrace);
						}
						$errorProperty->setValue($e, array_merge($newTrace, [
							[
								"function" => Terminal::$COLOR_YELLOW . "--- below is the original stack trace ---" . Terminal::$FORMAT_RESET,
							],
						], $oldTrace));
					}
					throw $e;
				}
			}
		};

		$this->addQuery($queryId, $request, $argv);
	}


	/**
	 * @param int $queryId
	 * @param class-string<Request> $request
	 * @param array $argv
	 * @return void
	 * @throws QueueShutdownException
	 */
	private function addQuery(int $queryId, string $request, array $argv = []) : void{
		$this->bufferSend->scheduleQuery($queryId, $request, $argv);

		$iterator = $this->workers->getIterator();
		while ($iterator->valid()) {
			$worker = $iterator->current();
			if(!$worker || !$worker->isBusy()){
				return;
			}
			$iterator->next();
		}
		unset($iterator);
		if($this->workerCount < $this->workers->getSize()){
			$this->addWorker();
		}
	}



	private function reportError(?callable $default, MongoError $error, ?Exception $trace) : void{
		if($default !== null){
			try{
				$default($error, $trace);
				$error = null;
			}catch(MongoError $err){
				$error = $err;
			}
		}
		if($error !== null){
			$this->logger->error($error->getMessage());
			if($error->getArgs() !== null){
				$this->logger->debug("Args: " . json_encode($error->getArgs()));
			}
			if($trace !== null){
				$this->logger->debug("Stack trace: " . $trace->getTraceAsString());
			}
		}
	}


	public function join(): void {
		/** @var ThreadMongodm[] $worker */
		foreach($this->workers as $worker) {
			$worker->join();
		}
	}

	public function readResults(array &$callbacks, ?int $expectedResults): void {
		if($expectedResults === null ){
			$resultsLists = $this->bufferRecv->fetchAllResults();
		} else {
			$resultsLists = $this->bufferRecv->waitForResults($expectedResults);
		}
		foreach($resultsLists as [$queryId, $results]) {
			if(!isset($callbacks[$queryId])) {
				throw new \InvalidArgumentException("Missing handler for query (#$queryId)");
			}
			$callbacks[$queryId]($results);
			unset($callbacks[$queryId]);
		}
	}

	public function connCreated() : bool{
		return $this->workers[0]->connCreated();
	}

	public function hasConnError() : bool{
		return $this->workers[0]->hasConnError();
	}

	public function getConnError() : ?string{
		return $this->workers[0]->getConnError();
	}

	public function waitAll() : void{
		while(!empty($this->handlers)){
			$this->readResults($this->handlers, count($this->handlers));
		}
	}

	public function checkResults() : void{
		$this->readResults($this->handlers, null);
	}

	public function getLoad() : float{
		return $this->bufferSend->count() / (float) $this->workers->getSize();
	}
}