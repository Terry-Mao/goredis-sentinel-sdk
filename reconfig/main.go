package main

import (
	"bytes"
	log "code.google.com/p/log4go"
	"path"
	//"compress/flate"
	"container/list"
	"encoding/json"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"strings"
	"time"
)

const (
	// SCRIPTS EXECUTION
	//
	// sentinel notification-script and sentinel reconfig-script are used in order
	// to configure scripts that are called to notify the system administrator
	// or to reconfigure clients after a failover. The scripts are executed
	// with the following rules for error handling:
	//
	// If script exists with "1" the execution is retried later (up to a maximum
	// number of times currently set to 10).
	//
	// If script exists with "2" (or an higher value) the script execution is
	// not retried.
	//
	// If script terminates because it receives a signal the behavior is the same
	// as exit code 1.
	//
	// A script has a maximum running time of 60 seconds. After this limit is
	// reached the script is terminated with a SIGKILL and the execution retried.
	termExit  = 2
	retryExit = 1
	okExit    = 0

	// sentinel role
	roleMaster = "master"
	roleSlave  = "slave"
)

var (
	sentinelTimeout = time.Millisecond * 500
	wait            = time.Millisecond * 500
	zkTimeout       = 30 * time.Second
	logWait         = time.Second * 1
	sentinelList    = list.New()
)

func main() {
	// init cmd args
	flag.Parse()
	// init config
	if err := initConfig(); err != nil {
		os.Exit(termExit)
	}
	// init log
	log.LoadConfiguration(conf.Log)
	defer log.Close()
	if len(conf.Sentinel) == 0 {
		// set to zk
		log.Error("conf.Sentinel don't have any sentinel addr")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	log.Info("sentinels: \"%v\", master: \"%s\"", conf.Sentinel, conf.Master)
	// init sentinel addrs
	for _, addr := range conf.Sentinel {
		sentinelList.PushBack(addr)
	}
	redisConn := sentinel()
	if redisConn == nil {
		log.Error("sentinel can't connect")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	defer redisConn.Close()
	// master redis
	redisMaster := masterAddr(redisConn)
	log.Debug("redis master: \"%s\"", redisMaster)
	isMaster := checkRole(redisMaster, roleMaster)
	if isMaster {
		refreshSentinel(redisConn)
	} else {
		log.Warn("abort the mission, master not ok")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	// TODO
	// slaves redis
	// sentinel reconfig state:
	// state failover add slave is not safe for client read
	redisSlaves := slaveAddrs(redisConn)
	if redisSlaves == nil {
		log.Error("slaves can't get")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	// marshal data
	nodeData := marshal(redisMaster, redisSlaves)
	if nodeData == nil {
		log.Error("marshal data failed")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	// set to zk
	zkConn := zkDial()
	if zkConn == nil {
		log.Error("zookeeper can't connect")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	defer zkConn.Close()
	if err := zkData(zkConn, nodeData); err != nil {
		log.Error("zookeeper set data failed")
		time.Sleep(logWait)
		os.Exit(retryExit)
	}
	time.Sleep(logWait)
	os.Exit(okExit)
}

// sentinel get a sentinel conn.
func sentinel() redis.Conn {
	// Step 1: connecting to the first Sentinel
	// the client should iterate the list of Sentinel addresses.
	// For every address it should try to connect to the Sentinel,
	// using a short timeout (in the order of a few hundreds of milliseconds).
	// On errors or timeouts the next Sentinel address should be tried.
	for e := sentinelList.Front(); e != nil; e = e.Next() {
		sentinelAddr, ok := e.Value.(string)
		if !ok {
			panic("sentinel addr string assection failed")
		}
		log.Info("connect to sentinel: \"%s\"", sentinelAddr)
		conn, err := redis.DialTimeout("tcp", sentinelAddr, sentinelTimeout, sentinelTimeout, sentinelTimeout)
		if err != nil {
			log.Error("redis.DialTimeout(\"tcp\", \"%s\", %d) error(%v)", sentinelAddr, sentinelTimeout, err)
			continue
		} else {
			log.Info("put sentinel: \"%s\" to the first", sentinelAddr)
			sentinelList.MoveToBack(e)
			return conn
		}
	}
	return nil
}

// zk get a zookeeper conn.
func zkDial() *zk.Conn {
	conn, session, err := zk.Connect(conf.ZK, zkTimeout)
	if err != nil {
		log.Error("zk.Connect(\"%v\", %d) error(%v)", conf.ZK, zkTimeout, err)
		return nil
	}
	go func() {
		for {
			event := <-session
			log.Info("zookeeper get a event: %s", event.State.String())
		}
	}()
	return conn
}

// zkData create zookeeper path, if path exists ignore error, and set node data.
func zkData(conn *zk.Conn, data []byte) error {
	node := path.Join(conf.ZKPath, conf.Node)
	tpath := ""
	for _, str := range strings.Split(conf.ZKPath, "/")[1:] {
		tpath = path.Join(tpath, "/", str)
		log.Info("create zookeeper path: \"%s\"", tpath)
		_, err := conn.Create(tpath, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			if err == zk.ErrNodeExists {
				log.Warn("zk.create(\"%s\") exists", tpath)
			} else {
				log.Error("zk.create(\"%s\") error(%v)", tpath, err)
				return err
			}
		}
	}
	if _, err := conn.Create(node, data, 0, zk.WorldACL(zk.PermAll)); err != nil {
		if err == zk.ErrNodeExists {
			oData, stat, err := conn.Get(node)
			if err != nil {
				log.Error("zk.Get(\"%s\") error(%v)", node, err)
				return err
			}
			if bytes.Equal(oData, data) {
				log.Warn("zk data same, no change")
				return nil
			}
			if _, err = conn.Set(node, data, stat.Version); err != nil {
				log.Error("zk.Set(\"%s\", data, 0) error(%v)", node, err)
				return err
			}
			log.Info("zk update data: \"%s\"", node)
		} else {
			log.Error("zk.create(\"%s\") error(%v)", tpath, err)
			return err
		}
	}
	return nil
}

// masterAddr get a master addr.
func masterAddr(conn redis.Conn) string {
	// Step 2: ask for master address
	redisMasterPair, err := redis.Strings(conn.Do("SENTINEL", "get-master-addr-by-name", conf.Master))
	if err != nil {
		log.Error("conn.Do(\"SENTINEL\", \"get-master-addr-by-name\", \"%s\") error(%v)", conf.Master, err)
		return ""
	}
	log.Info("get new master redis addr: \"%v\"", redisMasterPair)
	if len(redisMasterPair) != 2 {
		return ""
	}
	return strings.Join(redisMasterPair, ":")
}

// slaveAddrs get slave addrs.
func slaveAddrs(conn redis.Conn) []string {
	replies, err := redis.Values(conn.Do("SENTINEL", "slaves", conf.Master))
	if err != nil {
		log.Error("conn.Do(\"SENTINEL\", \"slaves\", \"%s\") error(%v)", conf.Master, err)
		return nil
	}
	slaveAddrs := []string{}
	for _, reply := range replies {
		d, err := redis.Strings(reply, nil)
		if err != nil {
			log.Error("redis.Strings(reply, nil) error(%v)", err)
			continue
		}
		log.Info("slave info: \"%s\"", d)
		if len(d) < 5 {
			log.Error("slave parse error, length")
			continue
		}
		slaveAddr := d[3] + ":" + d[5]
		if !checkRole(slaveAddr, roleSlave) {
			log.Warn("slave: \"%s\" not ok, ignore", slaveAddr)
			continue
		}
		log.Info("add slave addr: \"%s\"", slaveAddr)
		slaveAddrs = append(slaveAddrs, slaveAddr)
	}
	return slaveAddrs
}

// refreshSentinel get slave addrs.
func refreshSentinel(conn redis.Conn) {
	replies, err := redis.Values(conn.Do("SENTINEL", "sentinels", conf.Master))
	if err != nil {
		log.Error("conn.Do(\"SENTINEL\", \"sentinels\", \"%s\") error(%v)", conf.Master, err)
		return
	}
	newSentinelList := list.New()
	for _, reply := range replies {
		d, err := redis.Strings(reply, nil)
		if err != nil {
			log.Error("redis.Strings(reply, nil) error(%v)", err)
			return
		}
		log.Info("sentinel info: \"%s\"", d)
		if len(d) < 5 {
			log.Error("sentinel parse error, length")
			return
		}
		sentinelAddr := d[3] + ":" + d[5]
		newSentinel := true
		for e := sentinelList.Front(); e != nil; e = e.Next() {
			oldSentinelAddr, ok := e.Value.(string)
			if !ok {
				panic("sentinel addr string assection failed")
			}
			if oldSentinelAddr == sentinelAddr {
				newSentinel = false
				break
			}
		}
		if newSentinel {
			log.Info("add new sentinel addr: \"%s\"", sentinelAddr)
			newSentinelList.PushBack(sentinelAddr)
		} else {
			log.Info("ignore exist sentinel addr: \"%s\"", sentinelAddr)
		}
	}
	// add new sentinel list
	sentinelList.PushBackList(newSentinelList)
	// refresh config file
	sentinelConfig := []string{}
	for e := sentinelList.Front(); e != nil; e = e.Next() {
		sentinelAddr, ok := e.Value.(string)
		if !ok {
			panic("sentinel addr string assection failed")
		}
		sentinelConfig = append(sentinelConfig, sentinelAddr)
	}
	conf.Sentinel = sentinelConfig
	saveConfig()
}

// checkRole check the current redis role.
func checkRole(addr string, role string) bool {
	if addr == "" {
		return false
	}
	// Step 3: call the ROLE command in the target instance
	conn, err := redis.DialTimeout("tcp", addr, sentinelTimeout, sentinelTimeout, sentinelTimeout)
	if err != nil {
		log.Error("redis.DialTimeout(\"tcp\", \"%s\", 500ms...) error(%v)", addr, err)
		return false
	}
	defer conn.Close()
	replies, err := redis.Values(conn.Do("ROLE"))
	if err != nil {
		log.Error("conn.Do(\"ROLE\") error(%v)", err)
		return false
	}
	if len(replies) < 1 {
		return false
	}
	curRole, err := redis.String(replies[0], nil)
	if err != nil {
		log.Error("redis.String(replies[0], nil) error(%v)", err)
		return false
	}
	log.Info("redis: \"%s\" role: \"%s\"", addr, curRole)
	if curRole == role {
		return true
	}
	return false
}

// marshal marshal the node info data.
func marshal(master string, slaves []string) []byte {
	var nodeInfo struct {
		Weight int      `json:"w"`
		Master string   `json:"master"`
		Slaves []string `json:"slaves"`
	}
	nodeInfo.Weight = conf.Weight
	nodeInfo.Master = master
	nodeInfo.Slaves = slaves
	if data, err := json.Marshal(&nodeInfo); err != nil {
		log.Error("json.Marshal(d) error(%v)", err)
		return nil
	} else {
		log.Info("zk set data [%v]", string(data))
		// TODO compress
		return data
	}
}
