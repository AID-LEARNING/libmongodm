<?php

namespace SenseiTarzan\Mongodm\Thread;

use Composer\Autoload\ClassLoader;
use pmmp\thread\Thread as NativeThread;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\Thread;
use SenseiTarzan\Mongodm\Class\ETypeRequest;
use SenseiTarzan\Mongodm\Class\MongoConfig;
use SenseiTarzan\Mongodm\Class\MongoError;
use SenseiTarzan\Mongodm\Class\Request;
use SenseiTarzan\Mongodm\Client\MongoClient;
use pocketmine\Server;
use SenseiTarzan\Mongodm\libmongodm;
use Throwable;

class ThreadMongodm extends Thread
{
	private const MONGODB_TPS = 5;
	private const MONGODB_TIME_PER_TICK = 1 / self::MONGODB_TPS;
	private static int $nextSlaveNumber = 0;

	private readonly int $slaveId;
	private bool $busy = false;
	protected bool $connCreated = false;
	protected ?string $connError = null;

	public function __construct(
		private readonly SleeperHandlerEntry $sleeperEntry,
		private readonly string              $vendors,
		private readonly QuerySendQueue      $bufferSend,
		private readonly QueryRecvQueue      $bufferRecv,
		private readonly MongoConfig         $config
	)
	{
		$this->slaveId = self::$nextSlaveNumber++;
		if(!libmongodm::isPackaged()){
			/** @noinspection PhpUndefinedMethodInspection */
			/** @noinspection NullPointerExceptionInspection */
			/** @var ClassLoader $cl */
			$cl = Server::getInstance()->getPluginManager()->getPlugin("DEVirion")->getVirionClassLoader();
			$this->setClassLoaders([Server::getInstance()->getLoader(), $cl]);
		}
		$this->start(NativeThread::INHERIT_INI);
	}

	protected function onRun(): void
	{
		require_once $this->vendors . '/vendor/autoload.php';
        $runner_request = [];
		$notifier = $this->sleeperEntry->createNotifier();
		try {
			$client = new MongoClient($this->config);
			$this->connCreated = true;
		} catch (Throwable $exception){
			$this->connError = $exception;
			$this->connCreated = true;
			return;
		}
		while(true) {
			$start = microtime(true);
			$this->busy = true;
			for ($i = 0; $i < 100; ++$i){
				$row = $this->bufferSend->fetchQuery();
				if (!($row instanceof ThreadSafeArray)) {
					$this->busy = false;
					break 2;
				}
                /**
                 * @var class-string<Request>|Closure($client, $argv): mixed $request
                 */
                $queryId = $row[0];
                $type = ETypeRequest::tryFrom($row[1]);
                $request = $row[2];
                $argv = igbinary_unserialize($row[3]);
				try{
                    if ($type === ETypeRequest::STRING_CLASS) {
                        if (!(isset($runner_request[$request])))
                            $runner_request[$request] = $request::run(...);
                        $this->bufferRecv->publishResult($queryId, $runner_request[$request]::run($client, $argv));
                    }elseif ($type === ETypeRequest::CLOSURE) {
                        $this->bufferRecv->publishResult($queryId, $request::run($client, $argv));
                    } else {
                        throw new MongoError(MongoError::STAGE_EXECUTE, "type not implemented");
                    }
				}catch(MongoError $error){
					$this->bufferRecv->publishError($queryId, $error);
				}
				$notifier->wakeupSleeper();
			}
			$this->busy = false;
			$time = microtime(true) - $start;
			if($time < self::MONGODB_TIME_PER_TICK){
				@time_sleep_until(microtime(true) + self::MONGODB_TIME_PER_TICK - $time);
			}
		}
	}
	public function stopRunning(): void {
		$this->bufferSend->invalidate();
		parent::quit();
	}

	/**
	 * @return int
	 */
	public function getSlaveId(): int
	{
		return $this->slaveId;
	}

	public function connCreated() : bool{
		return $this->connCreated;
	}

	public function hasConnError() : bool{
		return $this->connError !== null;
	}

	public function getConnError() : ?string{
		return $this->connError;
	}

	/**
	 * @return bool
	 */
	public function isBusy() : bool{
		return $this->busy;
	}

	public function quit() : void{
		$this->stopRunning();
		parent::quit();
	}
}