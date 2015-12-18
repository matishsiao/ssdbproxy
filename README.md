# ssdbproxy

ssdb sharding proxy service by go lang.

# version

version: 0.0.5

# futures
	support functions:
		auth 
		set
		get
		del
		incr
		exists
		keys
		rkeys
		scan
		rscan
		multi_set
		multi_get
		multi_del
		hset
		hget
		hdel
		hincr
		hexists
		hsize
		hlist
		hrlist
		hkeys
		hgetall
		hscan
		hrscan
		hclear
		multi_hset
		multi_hget
		multi_hdel
	
#support gzip:
	
if you send command "zip" with args "1" to proxy server, proxy server will retrun base64 encode gzip string

golang example code:

https://github.com/matishsiao/gossdb

	
# configuration

use json format to configuration proxy setting.
	
```
	{
	  "debug":true,
	  "host":"127.0.0.1", //Proxy listen host
	  "port":4001,// Proxy listen port
	  "sync":false,//show db mirror status
	  "password":"", //Proxy password
	  "nodelist":[ //Sharding nodes
	    {
	      "id":"current", 
	      "host":"127.0.0.1",
	      "port":4002,
	      "password":"ssdbpassword",
	      "mode":"main",//current db
	      "weight":100
	    },
	    {
	      "id":"db2",
	      "host":"127.0.0.1",
	      "port":4001,
	      "password":"ssdbpassword",
	      "mode":"mirror",//all proxy set or delete command will auto sync up to this proxy server
	      "weight":100
	    },
	    {
	      "id":"asia",
	      "host":"127.0.0.1",
	      "port":4003,
	      "password":"ssdbpassword",
	      "mode":"queries",//queries slave db
	      "weight":100
	    }
	    ]
	}
```

| Column  | Description |
| ------------- | ------------- |
| debug  | debug mode:true / false  |
| host  | proxy listen host  |
| port  | proxy listen port  |
| sync  | proxy sync debug mode: true / false |
| password  | if you use auth params,you can use it to control connection |
| nodelist  | Default mode equal "main" is current db,so we will read/write in this node first.  |

#How to build

```
 go get github.com/matishsiao/ssdbproxy/
 cd $GOPATH/github.com/matishsiao/ssdbproxy
 go build
```

#see more information?
 http://matishsiao.blogspot.tw/2015/09/ssdb-sharding-proxy.html
