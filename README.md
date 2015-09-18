# ssdbproxy

ssdb sharding proxy service by go lang.

### futures
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

### configuration

use json format to configuration proxy setting.
	
```
	{
	  "debug":true,
	  "host":"127.0.0.1", //Proxy listen host
	  "port":4001,// Proxy listen port
	  "password":"", //Proxy password
	  "nodelist":[ //Sharding nodes
	    {
	      "id":"current", 
	      "host":"127.0.0.1",
	      "port":4002,
	      "password":"ssdbpassword",
	      "weight":100
	    },
	    {
	      "id":"asia",
	      "host":"127.0.0.1",
	      "port":4003,
	      "password":"ssdbpassword",
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
| password  | if you use auth params,you can use it to control connection |
| nodelist  | Default node 1 is current db,so we will read/write in this node first.  |
