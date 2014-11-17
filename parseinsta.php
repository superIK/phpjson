<?php

require_once('riak-php-client/src/Basho/Riak/Riak.php');
require_once('riak-php-client/src/Basho/Riak/Bucket.php');
require_once('riak-php-client/src/Basho/Riak/Exception.php');
require_once('riak-php-client/src/Basho/Riak/Link.php');
require_once('riak-php-client/src/Basho/Riak/MapReduce.php');
require_once('riak-php-client/src/Basho/Riak/Object.php');
require_once('riak-php-client/src/Basho/Riak/StringIO.php');
require_once('riak-php-client/src/Basho/Riak/Utils.php');
require_once('riak-php-client/src/Basho/Riak/Link/Phase.php');
require_once('riak-php-client/src/Basho/Riak/MapReduce/Phase.php');


// get_json('http://127.0.0.1:10018/riak/instagram/528962560205533186');

$client_source = new Basho\Riak\Riak('127.0.0.1', 10018);
$client_target = new Basho\Riak\Riak('127.0.0.1', 10018);
$bucket_source = $client_source->bucket('instagram');
get_keys();

function get_keys() {
global $bucket_source, $client_source;
$string = file_get_contents('http://127.0.0.1:10018/buckets/instagram/keys?keys=true');
$tweet = json_decode($string,true);

foreach($tweet['keys'] as $p) {
  $bucket_source = $client_source->bucket('instagram');
  $jsonurl = 'http://127.0.0.1:10018/riak/instagram/'.$p;
  get_json($jsonurl);
  $sourcetweet = $bucket_source->get($p);
  $sourcetweet->delete();
 }

}

function get_json($url) {
global $client_target;
$string = file_get_contents($url);
// echo $string;
$tweet = json_decode($string,true);
$bucket_target = $client_target->bucket('instagramtags2');

foreach($tweet['entities']['hashtags'] as $p)
{
//if ($tweet['lang'] = 'en' AND strpos($tweet['user']['location'],'UK')) {
if ($tweet['lang'] = 'en') {
 echo 'Hashtag: '.$p['text'];
 echo "\r\n";
 echo 'Time:  : '.$tweet['timestamp_ms'];
 echo "\r\n";
 echo 'Language: '.$tweet['lang'];
 echo "\r\n"; 
 echo 'Location: '.$tweet['coordinates'];
 echo "\r\n"; 
 echo 'Retweets: '.$tweet['retweet_count'];
 echo "\r\n";
 $riak_tweet = $bucket_target->newObject($tweet['id'], array(
	'hashtag' => $p['text'],
	'time' => $tweet['timestamp_ms'],
	'language' => $tweet['lang'],
	'location' => $tweet['coordinates']
 ));
 $riak_tweet->store(); }
}

// print_r($tweet);

}

?>
