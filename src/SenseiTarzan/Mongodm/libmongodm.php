<?php

namespace SenseiTarzan\Mongodm;

use pocketmine\plugin\PluginBase;
use pocketmine\utils\Terminal;
use SenseiTarzan\Mongodm\Class\MongoConfig;
use SenseiTarzan\Mongodm\Class\MongoError;
use SenseiTarzan\Mongodm\Thread\MongodmManager;
use Symfony\Component\Filesystem\Path;

class libmongodm
{



	/** @var bool */
	private static bool $packaged;

	public static function isPackaged() : bool{
		return self::$packaged;
	}

	public static function detectPackaged() : void{
		self::$packaged = __CLASS__ !== 'SenseiTarzan\Mongodm\libmongodm';

		if(!self::$packaged && defined("pocketmine\\VERSION")){
			echo Terminal::$COLOR_YELLOW . "Warning: Use of unshaded libmongodm detected. Debug mode is enabled. This may lead to major performance drop. Please use a shaded package in production. See https://poggit.pmmp.io/virion for more information.\n";
		}
	}

	/**
	 * @param PluginBase $plugin
	 * @param MongoConfig $configData
	 * @param int $workerLimit
	 * @return MongodmManager
	 */
	public static function create(PluginBase $plugin,  MongoConfig $configData, int $workerLimit = 2) : MongodmManager{
		$vendors = Path::join($plugin->getServer()->getDataPath(), "libmongodm");
		if(is_dir($vendors . "/vendor"))
			require_once $vendors . '/vendor/autoload.php';
		else {
			mkdir($vendors, 0777, true);
			throw new MongoError("libmongodm library not found");
		}
		libmongodm::detectPackaged();

		$manager = new MongodmManager($plugin, $vendors, $workerLimit, $configData);
		while(!$manager->connCreated()){
			usleep(1000);
		}
		if($manager->hasConnError()){
			throw new MongoError(MongoError::STAGE_CONNECT, $manager->getConnError());
		}
		return $manager;
	}
}