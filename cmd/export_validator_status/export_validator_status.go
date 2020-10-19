package main

import (
	"eth2-exporter/db"
	"eth2-exporter/eth2api"
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"flag"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	_ "eth2-exporter/docs"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	configPath := flag.String("config", "", "Path to the config file")
	flag.Parse()

	logrus.Printf("config file path: %v", *configPath)
	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)

	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg

	db.MustInitDB(cfg.Database.Username, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	defer db.DB.Close()

	logrus.Infof("database connection established")
	if utils.Config.Chain.SlotsPerEpoch == 0 || utils.Config.Chain.SecondsPerSlot == 0 || utils.Config.Chain.GenesisTimestamp == 0 {
		logrus.Fatal("invalid chain configuration specified, you must specify the slots per epoch, seconds per slot and genesis timestamp in the config file")
	}

	go func() {
		for {
			exportValidatorStatuses()
			time.Sleep(time.Second * 12)
		}
	}()

	utils.WaitForCtrlC()

	logrus.Println("exitting ...")
}

func exportValidatorStatuses() {
	var fromEpoch uint64
	err := db.DB.Get(&fromEpoch, "select coalesce(max(epoch),0) from epochs_status_stats")
	if err != nil {
		panic(err)
	}

	client, err := eth2api.NewClient(utils.Config.Indexer.Node.Host)
	if err != nil {
		panic(err)
	}

	h, err := client.GetHeader("head")
	if err != nil {
		panic(err)
	}

	toEpoch := h.Header.Message.Slot

	logrus.Infof("exporting %v-%v", fromEpoch, toEpoch)

	for i := int(fromEpoch); i <= int(toEpoch); i++ {
		t0 := time.Now()
		vs, err := client.GetValidators(fmt.Sprintf("%v", i*32))
		if err != nil {
			panic(err)
		}
		t1 := time.Now()
		err = saveValidators(uint64(i), vs)
		if err != nil {
			panic(err)
		}
		t2 := time.Now()
		logrus.WithFields(logrus.Fields{
			"epoch":      i,
			"validators": len(vs),
			"durGet":     t1.Sub(t0),
			"durSave":    t2.Sub(t1),
			"durAll":     t2.Sub(t0),
		}).Info("exported validator-statuses")
	}
}

func saveValidators(epoch uint64, validators []*eth2api.Validator) error {
	tx, err := db.DB.Begin()
	if err != nil {
		return fmt.Errorf("error starting db transactions: %v", err)
	}
	defer tx.Rollback()

	type statusStats struct {
		ValidatorsCount       uint64
		TotalBalance          uint64
		TotalEffectiveBalance uint64
	}
	statusStatsMap := map[string]*statusStats{}

	for _, v := range validators {
		status := string(v.Status)
		s, exists := statusStatsMap[status]
		if !exists {
			statusStatsMap[status] = &statusStats{
				ValidatorsCount:       1,
				TotalBalance:          v.Balance,
				TotalEffectiveBalance: v.Validator.EffectiveBalance,
			}
		} else {
			s.ValidatorsCount++
			s.TotalBalance += v.Balance
			s.TotalEffectiveBalance += v.Validator.EffectiveBalance
		}
	}

	stmt, err := tx.Prepare(`
		INSERT INTO epochs_status_stats (
			epoch,
			status,
			validators_count,
			total_balance,
			total_effective_balance
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (epoch, status) DO UPDATE SET
			epoch                   = excluded.epoch,
			status                  = excluded.status,
			validators_count        = excluded.validators_count,
			total_balance           = excluded.total_balance,
			total_effective_balance = excluded.total_effective_balance`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for status, stats := range statusStatsMap {
		// logrus.Infof("%v: %v: %v: %v: %v", epoch, status, stats.ValidatorsCount, stats.TotalBalance, stats.TotalEffectiveBalance)
		_, err = stmt.Exec(epoch, status, stats.ValidatorsCount, stats.TotalBalance, stats.TotalEffectiveBalance)
		if err != nil {
			return fmt.Errorf("error executing epochs_status_stats statement: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing db transaction: %v", err)
	}

	return nil
}
