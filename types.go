package main

import (
	"sort"
	"github.com/matishsiao/gossdb/ssdb"
)

type Configs struct {
	Debug		bool `json:"debug"`
	Host     string `json:"host"`
	Timeout	int64	`json:"timeout"`
	Sync	bool	`json:"sync"`
	Nodelist []DBNodeInfo `json:"nodelist"`
	Password string `json:"password"`
	Port     int    `json:"port"`
}

type DBNodeInfo struct {
	Host	string `json:"host"`
	Id		string `json:"id"`
	Password string `json:"password"`
	Port     int    `json:"port"`
	Weight   int    `json:"weight"`
	Mode	string	`json:"mode"`
}

type DBNode struct {
	Client *ssdb.Client
	Info DBNodeInfo
	Id string
}

type sortedMap struct {
	m map[string]string
	s []string
}

func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] > sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]string) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key, _ := range m {
		sm.s[i] = key
		i++
	}
	sort.Strings(sm.s)
	return sm.s
}