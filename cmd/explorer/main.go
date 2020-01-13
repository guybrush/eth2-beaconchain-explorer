package main

import (
	"eth2-exporter/db"
	"eth2-exporter/exporter"
	"eth2-exporter/handlers"
	"eth2-exporter/rpc"
	"eth2-exporter/services"
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	negronilogrus "github.com/meatballhat/negroni-logrus"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/sirupsen/logrus"
	"github.com/urfave/negroni"
	"github.com/zesik/proxyaddr"
)

func main() {
	configPath := flag.String("config", "", "Path to the config file")
	flag.Parse()

	log.Printf("Config file path: %v", *configPath)
	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)

	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	dbConn, err := sqlx.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", cfg.Database.Username, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name))
	if err != nil {
		log.Fatal(err)
	}

	// The golang sql driver does not properly implement PingContext
	// therefore we use a timer to catch db connection timeouts
	dbConnectionTimeout := time.NewTimer(15 * time.Second)
	go func() {
		<-dbConnectionTimeout.C
		log.Fatal("Timeout while connecting to the database")
	}()
	err = dbConn.Ping()
	if err != nil {
		log.Fatal(err)
	}
	dbConnectionTimeout.Stop()

	db.DB = dbConn
	defer db.DB.Close()

	utils.Config = cfg

	if utils.Config.Chain.SlotsPerEpoch == 0 || utils.Config.Chain.SecondsPerSlot == 0 || utils.Config.Chain.GenesisTimestamp == 0 {
		log.Fatal("Invalid chain configuration specified, you must specify the slots per epoch, seconds per slot and genesis timestamp in the config file")
	}

	if cfg.Indexer.Enabled {
		var rpcClient rpc.Client

		if utils.Config.Indexer.Node.Type == "prysm" {
			rpcClient, err = rpc.NewPrysmClient(cfg.Indexer.Node.Host + ":" + cfg.Indexer.Node.Port)
			if err != nil {
				log.Fatal(err)
			}
		} else if utils.Config.Indexer.Node.Type == "lighthouse" {
			rpcClient, err = rpc.NewLighthouseClient(cfg.Indexer.Node.Host + ":" + cfg.Indexer.Node.Port)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatalf("Invalid note type %v specified. Supported node types are prysm and lighthouse", utils.Config.Indexer.Node.Type)
		}

		go exporter.Start(rpcClient)
	}

	if cfg.Frontend.Enabled {
		services.Init() // Init frontend services

		router := mux.NewRouter()
		router.HandleFunc("/", handlers.Index).Methods("GET")
		router.HandleFunc("/index/data", handlers.IndexPageData).Methods("GET")
		router.HandleFunc("/block/{slotOrHash}", handlers.Block).Methods("GET")
		router.HandleFunc("/blocks", handlers.Blocks).Methods("GET")
		router.HandleFunc("/blocks/data", handlers.BlocksData).Methods("GET")
		router.HandleFunc("/vis", handlers.Vis).Methods("GET")
		router.HandleFunc("/charts", handlers.Charts).Methods("GET")
		router.HandleFunc("/charts/blocks", handlers.BlocksChart).Methods("GET")
		router.HandleFunc("/charts/validators", handlers.ActiveValidatorChart).Methods("GET")
		router.HandleFunc("/charts/staked_ether", handlers.StakedEtherChart).Methods("GET")
		router.HandleFunc("/charts/average_balance", handlers.AverageBalanceChart).Methods("GET")
		router.HandleFunc("/vis/blocks", handlers.VisBlocks).Methods("GET")
		router.HandleFunc("/vis/votes", handlers.VisVotes).Methods("GET")
		router.HandleFunc("/epoch/{epoch}", handlers.Epoch).Methods("GET")
		router.HandleFunc("/epochs", handlers.Epochs).Methods("GET")
		router.HandleFunc("/epochs/data", handlers.EpochsData).Methods("GET")
		router.HandleFunc("/validator/{index}", handlers.Validator).Methods("GET")
		router.HandleFunc("/validator/{index}/proposedblocks", handlers.ValidatorProposedBlocks).Methods("GET")
		router.HandleFunc("/validator/{index}/attestations", handlers.ValidatorAttestations).Methods("GET")
		router.HandleFunc("/validators", handlers.Validators).Methods("GET")
		router.HandleFunc("/validators/data/pending", handlers.ValidatorsDataPending).Methods("GET")
		router.HandleFunc("/validators/data/active", handlers.ValidatorsDataActive).Methods("GET")
		router.HandleFunc("/validators/data/ejected", handlers.ValidatorsDataEjected).Methods("GET")
		router.HandleFunc("/dashboard", handlers.Dashboard).Methods("GET")
		router.HandleFunc("/dashboard/data/balance", handlers.DashboardDataBalance).Methods("GET")
		router.HandleFunc("/dashboard/data/balance2", handlers.DashboardDataBalance2).Methods("GET")
		router.HandleFunc("/dashboard/data/proposals", handlers.DashboardDataProposals).Methods("GET")
		router.HandleFunc("/dashboard/data/validators", handlers.DashboardDataValidators).Methods("GET")
		router.HandleFunc("/dashboard/data/pending", handlers.DashboardDataValidatorsPending).Methods("GET")
		router.HandleFunc("/dashboard/data/active", handlers.DashboardDataValidatorsActive).Methods("GET")
		router.HandleFunc("/dashboard/data/ejected", handlers.DashboardDataValidatorsEjected).Methods("GET")
		router.HandleFunc("/search", handlers.Search).Methods("POST")
		router.HandleFunc("/search/{type}/{search}", handlers.SearchAhead).Methods("GET")
		router.HandleFunc("/faq", handlers.Faq).Methods("GET")
		router.HandleFunc("/imprint", handlers.Imprint).Methods("GET")

		router.PathPrefix("/").Handler(http.FileServer(http.Dir("static")))

		n := negroni.New(negroni.NewRecovery())

		// Customize the logging middleware to include a proper module entry for the frontend
		frontendLogger := negronilogrus.NewMiddleware()
		frontendLogger.Before = func(entry *logrus.Entry, request *http.Request, s string) *logrus.Entry {
			entry = negronilogrus.DefaultBefore(entry, request, s)
			return entry.WithField("module", "frontend")
		}
		frontendLogger.After = func(entry *logrus.Entry, writer negroni.ResponseWriter, duration time.Duration, s string) *logrus.Entry {
			entry = negronilogrus.DefaultAfter(entry, writer, duration, s)
			return entry.WithField("module", "frontend")
		}
		n.Use(frontendLogger)

		n.Use(gzip.Gzip(gzip.DefaultCompression))

		pa := &proxyaddr.ProxyAddr{}
		pa.Init(proxyaddr.CIDRLoopback)
		n.Use(pa)

		n.UseHandler(router)

		srv := &http.Server{
			Addr:         cfg.Frontend.Server.Host + ":" + cfg.Frontend.Server.Port,
			WriteTimeout: time.Second * 15,
			ReadTimeout:  time.Second * 15,
			IdleTimeout:  time.Second * 60,
			Handler:      n,
		}

		log.Printf("HTTP servicer listinging on %v", srv.Addr)
		go func() {
			if err := srv.ListenAndServe(); err != nil {
				log.Println(err)
			}
		}()
	}

	utils.WaitForCtrlC()

	log.Println("Exiting")
}
