<?php

include(dirname(__FILE__) .'/ssdb/SSDB.php');
set_time_limit(900);
ini_set('memory_limit', '1024M');
$host = '104.155.206.199';
//$host = '127.0.0.1';
$port = 4001;
$pwd = "sa23891odi1@8hfn!0932aqiomc9AQjiHH";


originTest($host,4004,$pwd);
proxyTest($host,4001,$pwd,true);

//use gzip
function proxyTest($host,$port,$pwd,$zip) {
	try{
		$ssdb = new SimpleSSDB($host, $port,120000);
	}catch(Exception $e){
		die(__LINE__ . ' ' . $e->getMessage());
	}
	$auth = $ssdb->auth($pwd);
	if ($auth == null) {
		//echo "auth success";
	} else {
		echo "auth failed";
		exit();
	}
	$data = null;
	if ($zip) {
		$result = $ssdb->zip("1");
	}
	$start = microtime(true);
	//$data = $ssdb->hscan("OneTableTest","","",2000);
	$data = $ssdb->hgetall("OneTableTest");
	$useTime = microtime(true) -$start;
	$len = count($data);
	echo "proxyTest use:$useTime len:$len\n";
}

function originTest($host,$port,$pwd) {
	try{
		$ssdb = new SimpleSSDB($host, $port,120000);
		//$ssdb->easy();
	}catch(Exception $e){
		die(__LINE__ . ' ' . $e->getMessage());
	}
	$auth = $ssdb->auth($pwd);
	if ($auth == null) {
		//echo "auth success";
	} else {
		echo "auth failed";
		exit();
	}
	$start = microtime(true);
	$data = $ssdb->hgetall("OneTableTest");
	$useTime = microtime(true) - $start;
	$len = count($data);
	echo "originTest use:$useTime len:$len\n";
}