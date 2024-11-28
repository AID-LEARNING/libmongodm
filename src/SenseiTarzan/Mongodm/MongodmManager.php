<?php

namespace SenseiTarzan\Mongodm;

use AttachableLogger;
use Error;
use Exception;
use Generator;
use pocketmine\plugin\Plugin;
use pocketmine\Server;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\utils\Terminal;
use ReflectionClass;
use SenseiTarzan\Mongodm\Class\MongoConfig;
use SenseiTarzan\Mongodm\Class\MongoError;
use SenseiTarzan\Mongodm\Class\Request;
use SenseiTarzan\Mongodm\Class\Response;
use SenseiTarzan\Mongodm\Exception\QueueShutdownException;
use SenseiTarzan\Mongodm\Thread\QueryRecvQueue;
use SenseiTarzan\Mongodm\Thread\QuerySendQueue;
use SenseiTarzan\Mongodm\Thread\ThreadMongodm;
use SOFe\AwaitGenerator\Await;
use SplFixedArray;

class MongodmManager
{
	private SleeperHandlerEntry $sleeperHandlerEntry;
	/**
	 * @var array<ThreadMongodm>
	 */
	private array $workers;
	private QuerySendQueue $bufferSend;
	private QueryRecvQueue $bufferRecv;
	private readonly AttachableLogger $logger;

	private array $handlers = [];
	private int $workerCount = 0;
	private static int $queryId = 0;

	public function __construct(
		Plugin                       $plugin,
		private string               $vendors,
		readonly int                 $workerCountMax,
		private readonly MongoConfig $config
	)
	{
		$this->logger = $plugin->getLogger();
		$this->workers = [];
		$this->bufferSend = new QuerySendQueue();
		$this->bufferRecv = new QueryRecvQueue();
		$this->sleeperHandlerEntry = Server::getInstance()->getTickSleeper()->addNotifier(function(): void {
			$this->checkResults();
		});
		$this->addWorker();
	}

	private function addWorker(): void
	{
		$this->workers[$this->workerCount++] =  new ThreadMongodm($this->sleeperHandlerEntry, $this->vendors, $this->bufferSend, $this->bufferRecv, $this->config);
	}

	public function stopRunning(): void
	{
		//$this->bufferSend->invalidate();
		/** @var ThreadMongodm[] $worker */
		foreach($this->workers as $worker) {
			$worker?->stopRunning();
		}
	}

	public function close(): void
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
	 * @param string $request
	 * @param array $argv
	 * @return Generator
	 * @throws QueueShutdownException
	 */
	public function asyncRequest(string $request, array $argv = []): Generator
	{
		$onSuccess = yield Await::RESOLVE;
		$onError = yield Await::REJECT;
		$this->executeRequest($request, $argv, $onSuccess, $onError);
		return yield Await::ONCE;
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

		foreach($this->workers as $worker) {
			if(!$worker->isBusy())
				return;
		}
		if($this->workerCount < $this->workerCountMax){
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