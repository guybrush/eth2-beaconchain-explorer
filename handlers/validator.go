package handlers

import (
	"encoding/hex"
	"encoding/json"
	"eth2-exporter/db"
	"eth2-exporter/services"
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"eth2-exporter/version"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

var validatorTemplate = template.Must(template.New("validator").Funcs(utils.GetTemplateFuncs()).ParseFiles("templates/layout.html", "templates/validator.html"))
var validatorNotFoundTemplate = template.Must(template.New("validatornotfound").ParseFiles("templates/layout.html", "templates/validatornotfound.html"))

// Validator returns validator data using a go template
func Validator(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	var index uint64
	var err error

	validatorPageData := types.ValidatorPageData{}

	data := &types.PageData{
		Meta: &types.Meta{
			Description: "beaconcha.in makes the Ethereum 2.0. beacon chain accessible to non-technical end users",
		},
		ShowSyncingMessage: services.IsSyncing(),
		Active:             "validators",
		Data:               nil,
		Version:            version.Version,
	}

	if strings.Contains(vars["index"], "0x") || len(vars["index"]) == 96 {
		pubKey, err := hex.DecodeString(strings.Replace(vars["index"], "0x", "", -1))
		if err != nil {
			logger.Errorf("error parsing validator public key %v: %v", vars["index"], err)
			http.Error(w, "Internal server error", 503)
			return
		}

		index, err = db.GetValidatorIndex(pubKey)
		if err != nil {
			data.Meta.Title = fmt.Sprintf("%v - Validator %x - beaconcha.in - %v", utils.Config.Frontend.SiteName, pubKey, time.Now().Year())
			data.Meta.Path = fmt.Sprintf("/validator/%v", index)
			err := validatorNotFoundTemplate.ExecuteTemplate(w, "layout", data)

			if err != nil {
				logger.Errorf("error executing template for %v route: %v", r.URL.String(), err)
				http.Error(w, "Internal server error", 503)
				return
			}
			return
		}
	} else {
		index, err = strconv.ParseUint(vars["index"], 10, 64)
		if err != nil {
			logger.Errorf("error parsing validator index: %v", err)
			http.Error(w, "Internal server error", 503)
			return
		}
	}

	data.Meta.Title = fmt.Sprintf("%v - Validator %v - beaconcha.in - %v", utils.Config.Frontend.SiteName, index, time.Now().Year())
	data.Meta.Path = fmt.Sprintf("/validator/%v", index)

	err = db.DB.Get(&validatorPageData, `SELECT 
											validators.validatorindex, 
											validators.withdrawableepoch, 
											validators.effectivebalance, 
											validators.slashed, 
											validators.activationeligibilityepoch, 
											validators.activationepoch, 
											validators.exitepoch,
											validators.lastattestationslot,
											COALESCE(validator_balances.balance, 0) AS balance
										FROM validators
										LEFT JOIN validator_balances 
											ON validators.validatorindex = validator_balances.validatorindex
											AND validator_balances.epoch = $1
										WHERE validators.validatorindex = $2
										LIMIT 1`, services.LatestEpoch(), index)
	if err != nil {
		logger.Errorf("error retrieving validator page data: %v", err)

		err := validatorNotFoundTemplate.ExecuteTemplate(w, "layout", data)

		if err != nil {
			logger.Errorf("error executing template for %v route: %v", r.URL.String(), err)
			http.Error(w, "Internal server error", 503)
			return
		}
		return
	}

	validatorPageData.ChainGenesisTimestamp = utils.Config.Chain.GenesisTimestamp
	validatorPageData.ChainSecondsPerSlot = utils.Config.Chain.SecondsPerSlot
	validatorPageData.ChainSlotsPerEpoch = utils.Config.Chain.SlotsPerEpoch
	validatorPageData.Epoch = services.LatestEpoch()
	validatorPageData.Index = index
	validatorPageData.PublicKey, err = db.GetValidatorPublicKey(index)
	if err != nil {
		logger.Errorf("error retrieving validator public key %v: %v", index, err)

		err := validatorNotFoundTemplate.ExecuteTemplate(w, "layout", data)

		if err != nil {
			logger.Errorf("error executing template for %v route: %v", r.URL.String(), err)
			http.Error(w, "Internal server error", 503)
			return
		}
		return
	}

	validatorPageData.ActivationEligibilityTs = utils.EpochToTime(validatorPageData.ActivationEligibilityEpoch)
	validatorPageData.ActivationTs = utils.EpochToTime(validatorPageData.ActivationEpoch)
	validatorPageData.ExitTs = utils.EpochToTime(validatorPageData.ExitEpoch)
	validatorPageData.WithdrawableTs = utils.EpochToTime(validatorPageData.WithdrawableEpoch)

	proposals := []struct {
		Day    uint64
		Status uint64
		Count  uint
	}{}

	err = db.DB.Select(&proposals, "select slot / $1 as day, status, count(*) FROM blocks WHERE proposer = $2 group by day, status order by day;", 86400/utils.Config.Chain.SecondsPerSlot, index)
	if err != nil {
		logger.Errorf("error retrieving Daily Proposed Blocks blocks count: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	for i := 0; i < len(proposals); i++ {
		if proposals[i].Status == 1 {
			validatorPageData.DailyProposalCount = append(validatorPageData.DailyProposalCount, types.DailyProposalCount{
				Day:      utils.SlotToTime(proposals[i].Day * 86400 / utils.Config.Chain.SecondsPerSlot).Unix(),
				Proposed: proposals[i].Count,
				Missed:   0,
				Orphaned: 0,
			})
		} else if proposals[i].Status == 2 {
			validatorPageData.DailyProposalCount = append(validatorPageData.DailyProposalCount, types.DailyProposalCount{
				Day:      utils.SlotToTime(proposals[i].Day * 86400 / utils.Config.Chain.SecondsPerSlot).Unix(),
				Proposed: 0,
				Missed:   proposals[i].Count,
				Orphaned: 0,
			})
		} else if proposals[i].Status == 3 {
			validatorPageData.DailyProposalCount = append(validatorPageData.DailyProposalCount, types.DailyProposalCount{
				Day:      utils.SlotToTime(proposals[i].Day * 86400 / utils.Config.Chain.SecondsPerSlot).Unix(),
				Proposed: 0,
				Missed:   0,
				Orphaned: proposals[i].Count,
			})
		} else {
			logger.Errorf("error parsing Daily Proposed Blocks unknown status: %v", proposals[i].Status)
		}
	}

	cutoff1d := time.Now().Add(time.Hour * 24 * -1)
	cutoff7d := time.Now().Add(time.Hour * 24 * 7 * -1)
	cutoff31d := time.Now().Add(time.Hour * 24 * 31 * -1)
	cutoff1dSlot := utils.TimeToSlot(uint64(cutoff1d.Unix()))
	cutoff7dSlot := utils.TimeToSlot(uint64(cutoff7d.Unix()))
	cutoff31dSlot := utils.TimeToSlot(uint64(cutoff31d.Unix()))
	missedStatusSlot := utils.TimeToSlot(uint64(time.Now().Add(time.Minute * -1).Unix()))

	proposedBlocks := []struct {
		Slot   uint64
		Status uint64
	}{}
	err = db.DB.Select(&proposedBlocks, `
		SELECT slot, status
		FROM blocks
		WHERE proposer = $1`, index)
	if err != nil {
		logger.Errorf("error retrieving proposed blocks: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	validatorPageData.ProposedBlocks = proposedBlocks

	proposedBlocksCount := []struct {
		Count  uint64
		Status uint64
		Cutoff uint64
	}{}
	err = db.DB.Select(&proposedBlocksCount, `
		SELECT
			COUNT(*),
			status,
			CASE
				WHEN slot > $2 THEN 1
				WHEN slot > $3 THEN 7
				WHEN slot > $4 THEN 31
				ELSE 0
			END AS cutoff
		FROM blocks
		WHERE proposer = $1
		GROUP BY status, cutoff`,
		index,
		cutoff1dSlot,
		cutoff7dSlot,
		cutoff31dSlot)
	if err != nil {
		logger.Errorf("error retrieving proposed blocks count: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	// var orphanedProposedBlocksCount1d uint64
	// var orphanedProposedBlocksCount7d uint64
	// var orphanedProposedBlocksCount31d uint64
	var missedProposedBlocksCount1d uint64
	var missedProposedBlocksCount7d uint64
	var missedProposedBlocksCount31d uint64
	var executedProposedBlocksCount1d uint64
	var executedProposedBlocksCount7d uint64
	var executedProposedBlocksCount31d uint64
	for _, c := range proposedBlocksCount {
		validatorPageData.TotalProposedBlocksCount += c.Count
		switch c.Status {
		case 3:
			switch c.Cutoff {
			// case 1:
			// 	orphanedProposedBlocksCount1d = c.Count
			// 	fallthrough
			// case 7:
			// 	orphanedProposedBlocksCount7d = c.Count
			// 	fallthrough
			// case 31:
			// 	orphanedProposedBlocksCount31d = c.Count
			// 	fallthrough
			default:
				validatorPageData.OrphanedProposedBlocksCount += c.Count
			}
		case 2:
			switch c.Cutoff {
			case 1:
				missedProposedBlocksCount1d += c.Count
				fallthrough
			case 7:
				missedProposedBlocksCount7d += c.Count
				fallthrough
			case 31:
				missedProposedBlocksCount31d += c.Count
				fallthrough
			default:
				validatorPageData.MissedProposedBlocksCount += c.Count
			}
		case 1:
			switch c.Cutoff {
			case 1:
				executedProposedBlocksCount1d += c.Count
				fallthrough
			case 7:
				executedProposedBlocksCount7d += c.Count
				fallthrough
			case 31:
				executedProposedBlocksCount31d += c.Count
				fallthrough
			default:
				validatorPageData.ExecutedProposedBlocksCount += c.Count
			}
		case 0:
			validatorPageData.ScheduledProposedBlocksCount += c.Count
		default:
			logger.Errorf("error retrieving proposed blocks count: unknown status: %v", c.Status)
		}
	}

	validatorPageData.ProposedBlocksEffectivenessTotal = utils.CalculateSuccessRate(validatorPageData.ExecutedProposedBlocksCount, validatorPageData.MissedProposedBlocksCount)
	validatorPageData.ProposedBlocksEffectiveness31d = utils.CalculateSuccessRate(executedProposedBlocksCount31d, missedProposedBlocksCount31d)
	validatorPageData.ProposedBlocksEffectiveness7d = utils.CalculateSuccessRate(executedProposedBlocksCount7d, missedProposedBlocksCount7d)
	validatorPageData.ProposedBlocksEffectiveness1d = utils.CalculateSuccessRate(executedProposedBlocksCount1d, missedProposedBlocksCount1d)

	missedAttestations := []uint64{}
	err = db.DB.Select(&missedAttestations, `
		SELECT attesterslot*$3+$4
		FROM attestation_assignments
		WHERE validatorindex = $1 AND ( status = 2 OR ( status = 0 AND attesterslot < $2 ) )`,
		index,
		missedStatusSlot,
		utils.Config.Chain.SecondsPerSlot,
		utils.Config.Chain.GenesisTimestamp)
	if err != nil {
		logger.Errorf("error retrieving missed attestations: %w", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	validatorPageData.MissedAttestations = missedAttestations

	attestationCounts := []struct {
		Count       uint64
		Fixedstatus uint64
		Cutoff      uint64
	}{}
	err = db.DB.Select(&attestationCounts, `
		SELECT
			COUNT(*),
			CASE
				WHEN status = 1 THEN 1
				WHEN status = 2 THEN 2
				WHEN status = 0 AND attesterslot < $2 THEN 2
				ELSE 0
			END AS fixedstatus,
			CASE
				WHEN attesterslot > $3 THEN 1
				WHEN attesterslot > $4 THEN 7
				WHEN attesterslot > $5 THEN 31
				ELSE 0
			END AS cutoff
		FROM attestation_assignments
		WHERE validatorindex = $1
		GROUP BY fixedstatus, cutoff`,
		index,
		missedStatusSlot,
		cutoff1dSlot,
		cutoff7dSlot,
		cutoff31dSlot)
	if err != nil {
		logger.Errorf("error retrieving attestation count: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	var missedAttestationsCount1d uint64
	var missedAttestationsCount7d uint64
	var missedAttestationsCount31d uint64
	var attestedAttestationsCount1d uint64
	var attestedAttestationsCount7d uint64
	var attestedAttestationsCount31d uint64
	for _, c := range attestationCounts {
		validatorPageData.TotalAttestationsCount += c.Count
		switch c.Fixedstatus {
		case 2:
			switch c.Cutoff {
			case 1:
				missedAttestationsCount1d += c.Count
				fallthrough
			case 7:
				missedAttestationsCount7d += c.Count
				fallthrough
			case 31:
				missedAttestationsCount31d += c.Count
				fallthrough
			default:
				validatorPageData.MissedAttestationsCount += c.Count
			}
		case 1:
			switch c.Cutoff {
			case 1:
				attestedAttestationsCount1d += c.Count
				fallthrough
			case 7:
				attestedAttestationsCount7d += c.Count
				fallthrough
			case 31:
				attestedAttestationsCount31d += c.Count
				fallthrough
			default:
				validatorPageData.AttestedAttestationsCount += c.Count
			}
		case 0:
			validatorPageData.ScheduledAttestationsCount += c.Count
		default:
			logger.Errorf("error retrieving attestation count: unknown status: %v", c.Fixedstatus)
		}
	}

	// fmt.Printf("============ error attestations: 1d: %v, 7d: %v, 31d: %v, total: %v, %+v\n",
	// 	missedAttestationsCount1d,
	// 	missedAttestationsCount7d,
	// 	missedAttestationsCount31d,
	// 	validatorPageData.MissedAttestationsCount,
	// 	attestationCounts)

	validatorPageData.AttestationsEffectivenessTotal = utils.CalculateSuccessRate(validatorPageData.AttestedAttestationsCount, validatorPageData.MissedAttestationsCount)
	validatorPageData.AttestationsEffectiveness31d = utils.CalculateSuccessRate(attestedAttestationsCount31d, missedAttestationsCount31d)
	validatorPageData.AttestationsEffectiveness7d = utils.CalculateSuccessRate(attestedAttestationsCount7d, missedAttestationsCount7d)
	validatorPageData.AttestationsEffectiveness1d = utils.CalculateSuccessRate(attestedAttestationsCount1d, missedAttestationsCount1d)

	var balanceHistory []*types.ValidatorBalanceHistory
	err = db.DB.Select(&balanceHistory, "SELECT epoch, balance FROM validator_balances WHERE validatorindex = $1 ORDER BY epoch", index)
	if err != nil {
		logger.Errorf("error retrieving validator balance history: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	validatorPageData.BalanceHistoryChartData = make([][]float64, len(balanceHistory))

	for i, balance := range balanceHistory {
		balanceTs := utils.EpochToTime(balance.Epoch)

		if balanceTs.Before(cutoff1d) {
			validatorPageData.Income1d = int64(validatorPageData.CurrentBalance) - int64(balance.Balance)
		}
		if balanceTs.Before(cutoff7d) {
			validatorPageData.Income7d = int64(validatorPageData.CurrentBalance) - int64(balance.Balance)
		}
		if balanceTs.Before(cutoff31d) {
			validatorPageData.Income31d = int64(validatorPageData.CurrentBalance) - int64(balance.Balance)
		}

		validatorPageData.BalanceHistoryChartData[i] = []float64{float64(balanceTs.Unix() * 1000), float64(balance.Balance) / 1000000000}
	}

	if validatorPageData.Income7d == 0 {
		validatorPageData.Income7d = int64(validatorPageData.CurrentBalance) - int64(balanceHistory[0].Balance)
	}

	if validatorPageData.Income31d == 0 {
		validatorPageData.Income31d = int64(validatorPageData.CurrentBalance) - int64(balanceHistory[0].Balance)
	}

	validatorPageData.IncomeTotal = int64(validatorPageData.CurrentBalance) - int64(balanceHistory[0].Balance)

	var effectiveBalanceHistory []*types.ValidatorBalanceHistory
	err = db.DB.Select(&effectiveBalanceHistory, "SELECT epoch, COALESCE(effectivebalance, 0) as balance FROM validator_balances WHERE validatorindex = $1 ORDER BY epoch", index)
	if err != nil {
		logger.Errorf("error retrieving validator effective balance history: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	validatorPageData.EffectiveBalanceHistoryChartData = make([][]float64, len(effectiveBalanceHistory))
	for i, balance := range effectiveBalanceHistory {
		validatorPageData.EffectiveBalanceHistoryChartData[i] = []float64{float64(utils.EpochToTime(balance.Epoch).Unix() * 1000), float64(balance.Balance) / 1000000000}
	}

	var firstSlotOfPreviousEpoch uint64
	if services.LatestEpoch() < 1 {
		firstSlotOfPreviousEpoch = 0
	} else {
		firstSlotOfPreviousEpoch = (services.LatestEpoch() - 1) * utils.Config.Chain.SlotsPerEpoch
	}

	if validatorPageData.Epoch > validatorPageData.ExitEpoch {
		validatorPageData.Status = "Exited"
	} else if validatorPageData.Epoch < validatorPageData.ActivationEpoch {
		validatorPageData.Status = "Pending"
	} else if validatorPageData.Slashed {
		if validatorPageData.ActivationEpoch < services.LatestEpoch() && (validatorPageData.LastAttestationSlot == nil || *validatorPageData.LastAttestationSlot < firstSlotOfPreviousEpoch) {
			validatorPageData.Status = "SlashingOffline"
		} else {
			validatorPageData.Status = "Slashing"
		}
	} else {
		if validatorPageData.ActivationEpoch < services.LatestEpoch() && (validatorPageData.LastAttestationSlot == nil || *validatorPageData.LastAttestationSlot < firstSlotOfPreviousEpoch) {
			validatorPageData.Status = "ActiveOffline"
		} else {
			validatorPageData.Status = "Active"
		}
	}

	err = db.DB.Select(&validatorPageData.ZeroVotedEpochs, "SELECT epoch FROM epochs WHERE votedether = 0")
	if err != nil {
		logger.Errorf("error retrieving zero voted ether epochs: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	data.Data = validatorPageData

	if utils.IsApiRequest(r) {
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(data.Data)
	} else {
		w.Header().Set("Content-Type", "text/html")
		err = validatorTemplate.ExecuteTemplate(w, "layout", data)
	}

	if err != nil {
		logger.Errorf("error executing template for %v route: %v", r.URL.String(), err)
		http.Error(w, "Internal server error", 503)
		return
	}
}

// ValidatorProposedBlocks returns a validator's proposed blocks in json
func ValidatorProposedBlocks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	index, err := strconv.ParseUint(vars["index"], 10, 64)
	if err != nil {
		logger.Errorf("error parsing validator index: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	q := r.URL.Query()

	draw, err := strconv.ParseUint(q.Get("draw"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables data parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	start, err := strconv.ParseUint(q.Get("start"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables start parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	length, err := strconv.ParseUint(q.Get("length"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables length parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	if length > 100 {
		length = 100
	}

	var totalCount uint64

	err = db.DB.Get(&totalCount, "SELECT COUNT(*) FROM blocks WHERE proposer = $1", index)
	if err != nil {
		logger.Errorf("error retrieving proposed blocks count: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	var blocks []*types.IndexPageDataBlocks
	err = db.DB.Select(&blocks, `SELECT blocks.epoch, 
											    blocks.slot,  
											    blocks.proposer,  
											    blocks.blockroot, 
											    blocks.parentroot, 
											    blocks.attestationscount, 
											    blocks.depositscount, 
											    blocks.voluntaryexitscount, 
											    blocks.proposerslashingscount, 
											    blocks.attesterslashingscount, 
											    blocks.status 
										FROM blocks 
										WHERE blocks.proposer = $1
										ORDER BY blocks.slot DESC
										LIMIT $2 OFFSET $3`, index, length, start)

	if err != nil {
		logger.Errorf("error retrieving proposed blocks data: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	tableData := make([][]interface{}, len(blocks))
	for i, b := range blocks {
		tableData[i] = []interface{}{
			fmt.Sprintf("%v", b.Epoch),
			fmt.Sprintf("%v", b.Slot),
			fmt.Sprintf("%v", utils.FormatBlockStatus(b.Status)),
			fmt.Sprintf("%v", utils.SlotToTime(b.Slot).Unix()),
			fmt.Sprintf("%x", b.BlockRoot),
			fmt.Sprintf("%v", b.Attestations),
			fmt.Sprintf("%v", b.Deposits),
			fmt.Sprintf("%v / %v", b.Proposerslashings, b.Attesterslashings),
			fmt.Sprintf("%v", b.Exits),
		}
	}

	data := &types.DataTableResponse{
		Draw:            draw,
		RecordsTotal:    totalCount,
		RecordsFiltered: totalCount,
		Data:            tableData,
	}

	err = json.NewEncoder(w).Encode(data)
	if err != nil {
		logger.Errorf("error enconding json response for %v route: %v", r.URL.String(), err)
		http.Error(w, "Internal server error", 503)
		return
	}
}

// ValidatorAttestations returns a validators attestations in json
func ValidatorAttestations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	index, err := strconv.ParseUint(vars["index"], 10, 64)
	if err != nil {
		logger.Errorf("error parsing validator index: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	q := r.URL.Query()

	draw, err := strconv.ParseUint(q.Get("draw"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables data parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	start, err := strconv.ParseUint(q.Get("start"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables start parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	length, err := strconv.ParseUint(q.Get("length"), 10, 64)
	if err != nil {
		logger.Errorf("error converting datatables length parameter from string to int: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	if length > 100 {
		length = 100
	}

	// support multi-sort
	orderByMap := map[string]string{
		"2": "status",
		"3": "attesterslot",
	}
	orderBy := ""
	for i := 0; i < len(orderByMap); i++ {
		columnKey := q.Get(fmt.Sprintf("order[%v][column]", i))
		column, exists := orderByMap[columnKey]
		if !exists {
			continue
		}
		orderDir := q.Get(fmt.Sprintf("order[%v][dir]", i))
		if orderDir != "desc" && orderDir != "asc" {
			orderDir = "desc"
		}
		if orderBy == "" {
			orderBy = fmt.Sprintf("%v %v", column, orderDir)
		} else {
			orderBy = fmt.Sprintf("%v, %v %v", orderBy, column, orderDir)
		}
	}
	if orderBy == "" {
		orderBy = "attesterslot desc"
	}

	missedStatusSlot := utils.TimeToSlot(uint64(time.Now().Add(time.Minute * -1).Unix()))

	type countsType struct {
		Count       uint64
		Fixedstatus uint64
	}
	counts := []*countsType{}
	err = db.DB.Select(&counts, `
		SELECT
			COUNT(*),
			CASE
				WHEN status = 1 THEN 1
				WHEN status = 2 THEN 2
				WHEN status = 0 AND attesterslot < $2 THEN 2
				ELSE 0
			END AS fixedstatus
		FROM attestation_assignments
		WHERE validatorindex = $1
		GROUP BY fixedstatus`, index, missedStatusSlot)
	if err != nil {
		logger.Errorf("error retrieving attestation counts: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}
	var totalCount uint64
	var scheduledCount uint64
	var attestedCount uint64
	var missedCount uint64
	for _, c := range counts {
		totalCount += c.Count
		switch c.Fixedstatus {
		case 2:
			missedCount += c.Count
		case 1:
			attestedCount += c.Count
		default:
			scheduledCount += c.Count
		}
	}

	var blocks []*types.ValidatorAttestation
	err = db.DB.Select(&blocks, fmt.Sprintf(`
		SELECT
			epoch,
			attesterslot,
			committeeindex,
			CASE
				WHEN status = 1 THEN 1
				WHEN status = 2 THEN 2
				WHEN status = 0 AND attesterslot < $4 THEN 2
				ELSE 0
			END AS status
		FROM attestation_assignments
		WHERE validatorindex = $1
		ORDER BY %s
		LIMIT $2 OFFSET $3`, orderBy), index, length, start, missedStatusSlot)

	if err != nil {
		logger.Errorf("error retrieving validator attestations data: %v", err)
		http.Error(w, "Internal server error", 503)
		return
	}

	tableData := make([][]interface{}, len(blocks))
	for i, b := range blocks {
		tableData[i] = []interface{}{
			fmt.Sprintf("%v", b.Epoch),
			fmt.Sprintf("%v", b.AttesterSlot),
			fmt.Sprintf("%v", utils.FormatAttestationStatus(b.Status)),
			fmt.Sprintf("%v", utils.SlotToTime(b.AttesterSlot).Unix()),
			fmt.Sprintf("%v", b.CommitteeIndex),
		}
	}

	data := &types.DataTableResponse{
		Draw:            draw,
		RecordsTotal:    totalCount,
		RecordsFiltered: totalCount,
		Data:            tableData,
	}

	err = json.NewEncoder(w).Encode(data)
	if err != nil {
		logger.Errorf("error enconding json response for %v route: %v", r.URL.String(), err)
		http.Error(w, "Internal server error", 503)
		return
	}
}
