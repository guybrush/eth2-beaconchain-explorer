package main

import (
	"encoding/base64"
	"encoding/json"
	"eth2-exporter/db"
	"eth2-exporter/utils"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/sirupsen/logrus"
)

func main() {
	fUser := flag.String("fUser", "", "fUser")
	fPass := flag.String("fPass", "", "fPass")
	fHost := flag.String("fHost", "", "fHost")
	fPort := flag.String("fPort", "", "fPort")
	fName := flag.String("fName", "", "fName")
	flag.Parse()

	db.MustInitDB(*fUser, *fPass, *fHost, *fPort, *fName)
	defer db.DB.Close()

	getMedallaPoapAttesters()
}

func getPoapProposers() {
	res := struct {
		Data []interface{}
	}{}
	get("https://beaconcha.in/poap/data")
}

func get(url string, res interface{}) error {
	client := &http.Client{Timeout: time.Second * 10}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error-response: %s", data)
	}

	return json.Unmarshal(data, res)
}

func getMedallaPoapAttesters() {
	result := []struct {
		Graffiti         string `db:"graffiti"`
		Blockcount       uint64 `db:"blockcount"`
		AttestationCount uint64 `db:"attestationcount"`
	}{}
	err := db.DB.Select(&result, `
		with
			recovervalidators as (
				select validatorindex, count(*) 
				from attestation_assignments a
				where attesterslot > 75000 and attesterslot < 115000 and status = '1'
				group by a.validatorindex
			)
		select graffiti, count(*) as blockcount, sum(v.count) as attestationcount
		from blocks
		inner join recovervalidators v on v.validatorindex = blocks.proposer
		where graffiti like 'poap%'
		group by graffiti
		order by attestationcount desc;`)
	if err != nil {
		logrus.Fatalf("error doing query: %v", err)
	}

	type resultItem struct {
		Eth1Address      string `json:"eth1address"`
		AttestationCount uint64 `json:"attestationcount"`
	}
	resultMap := map[string]resultItem{}

	for _, v := range result {
		e1addr, _, err := decodePoapGraffiti(v.Graffiti)
		if err != nil {
			logrus.WithField("graffiti", v.Graffiti).Errorf("error decoding poap-graffiti: %v", err)
			continue
		}
		item, exists := resultMap[e1addr]
		if !exists {
			resultMap[e1addr] = resultItem{e1addr, v.AttestationCount}
		} else {
			item.AttestationCount += v.AttestationCount
			resultMap[e1addr] = item
		}
	}

	i := 0
	resultArr := make([]resultItem, len(resultMap))
	for _, v := range resultMap {
		resultArr[i] = v
		i++
	}

	sort.Slice(resultArr, func(i, j int) bool {
		return resultArr[i].AttestationCount > resultArr[j].AttestationCount
	})

	res, err := json.Marshal(&resultArr)
	if err != nil {
		logrus.WithError(err).Fatal("cant marshal")
	}
	fmt.Printf(`%s`, res)
}

var poapClients = []string{"Prysm", "Lighthouse", "Teku", "Nimbus", "Lodestar"}

func decodePoapGraffiti(graffiti string) (eth1Address, client string, err error) {
	if len(graffiti) != 32 {
		return "", "", fmt.Errorf("invalid graffiti-length")
	}
	b, err := base64.StdEncoding.DecodeString(graffiti[4:])
	if err != nil {
		return "", "", fmt.Errorf("failed decoding base64: %w", err)
	}
	str := fmt.Sprintf("%x", b)
	if len(str) != 42 {
		return "", "", fmt.Errorf("invalid length")
	}
	eth1Address = "0x" + str[:40]
	if !utils.IsValidEth1Address(eth1Address) {
		return "", "", fmt.Errorf("invalid eth1-address: %v", eth1Address)
	}
	clientID, err := strconv.ParseInt(str[40:], 16, 64)
	if err != nil {
		return "", "", fmt.Errorf("invalid clientID: %v: %w", str[40:], err)
	}
	if clientID < 0 || int64(len(poapClients)) < clientID {
		return "", "", fmt.Errorf("invalid clientID: %v", str[40:])
	}
	return eth1Address, poapClients[clientID], nil
}
