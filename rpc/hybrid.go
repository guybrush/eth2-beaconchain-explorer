package rpc

import (
	"eth2-exporter/types"
	"eth2-exporter/utils"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// HybridClient connects to prysm and lighthouse
type HybridClient struct {
	prysmClient *PrysmClient
	lhClient    *LighthouseClient
	apiClient   *Eth2ApiClient
}

func NewHybridClient(prysmEndpoint, lhEndpoint, apiEndpoint string) (*HybridClient, error) {
	prysmClient, err := NewPrysmClient(prysmEndpoint)
	if err != nil {
		return nil, err
	}
	lhClient, err := NewLighthouseClient(lhEndpoint)
	if err != nil {
		return nil, err
	}
	apiClient, err := NewEth2ApiClient(apiEndpoint)
	if err != nil {
		return nil, err
	}
	c := &HybridClient{
		prysmClient: prysmClient,
		lhClient:    lhClient,
		apiClient:   apiClient,
	}
	return c, nil
}

func (c *HybridClient) GetChainHead() (*types.ChainHead, error) {
	return c.prysmClient.GetChainHead()
}

func (c *HybridClient) GetEpochData(epoch uint64) (*types.EpochData, error) {
	var err error

	t0 := time.Now()
	data := &types.EpochData{}
	data.Epoch = epoch

	slotStr := fmt.Sprintf("%d", epoch*utils.Config.Chain.SlotsPerEpoch)

	data.ValidatorAssignmentes, err = c.GetEpochAssignments(epoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving assignments for epoch %v: %w", epoch, err)
	}

	t1 := time.Now()

	// Retrieve all blocks for the epoch
	data.Blocks = make(map[uint64]map[string]*types.Block)

	for slot := epoch * utils.Config.Chain.SlotsPerEpoch; slot <= (epoch+1)*utils.Config.Chain.SlotsPerEpoch-1; slot++ {
		blocks, err := c.GetBlocksBySlot(slot)
		if err != nil {
			return nil, err
		}
		for _, block := range blocks {
			if data.Blocks[block.Slot] == nil {
				data.Blocks[block.Slot] = make(map[string]*types.Block)
			}
			data.Blocks[block.Slot][fmt.Sprintf("%x", block.BlockRoot)] = block
		}
	}

	// Fill up missed and scheduled blocks
	for slot, proposer := range data.ValidatorAssignmentes.ProposerAssignments {
		_, found := data.Blocks[slot]
		if !found {
			// Proposer was assigned but did not yet propose a block
			data.Blocks[slot] = make(map[string]*types.Block)
			data.Blocks[slot]["0x0"] = &types.Block{
				Status:            0,
				Proposer:          proposer,
				BlockRoot:         []byte{0x0},
				Slot:              slot,
				ParentRoot:        []byte{},
				StateRoot:         []byte{},
				Signature:         []byte{},
				RandaoReveal:      []byte{},
				Graffiti:          []byte{},
				BodyRoot:          []byte{},
				Eth1Data:          &types.Eth1Data{},
				ProposerSlashings: make([]*types.ProposerSlashing, 0),
				AttesterSlashings: make([]*types.AttesterSlashing, 0),
				Attestations:      make([]*types.Attestation, 0),
				Deposits:          make([]*types.Deposit, 0),
				VoluntaryExits:    make([]*types.VoluntaryExit, 0),
			}

			if utils.SlotToTime(slot).After(time.Now().Add(time.Second * -60)) {
				// Block is in the future, set status to scheduled
				data.Blocks[slot]["0x0"].Status = 0
				data.Blocks[slot]["0x0"].BlockRoot = []byte{0x0}
			} else {
				// Block is in the past, set status to missed
				data.Blocks[slot]["0x0"].Status = 2
				data.Blocks[slot]["0x0"].BlockRoot = []byte{0x1}
			}
		}
	}

	t2 := time.Now()

	// Retrieve the validator set for the epoch

	validators, err := c.apiClient.client.GetValidators(slotStr)
	if err != nil {
		return nil, err
	}

	data.Validators = make([]*types.Validator, len(validators))

	for i, validator := range validators {
		data.Validators[i] = &types.Validator{
			Index:                      validator.Index,
			PublicKey:                  validator.Validator.Pubkey,
			WithdrawalCredentials:      validator.Validator.WithdrawalCredentials,
			Balance:                    validator.Balance,
			EffectiveBalance:           validator.Validator.EffectiveBalance,
			Slashed:                    validator.Validator.Slashed,
			ActivationEligibilityEpoch: validator.Validator.ActivationEligibilityEpoch,
			ActivationEpoch:            validator.Validator.ActivationEpoch,
			ExitEpoch:                  validator.Validator.ExitEpoch,
			WithdrawableEpoch:          validator.Validator.WithdrawableEpoch,
		}
	}

	t3 := time.Now()

	data.EpochParticipationStats, err = c.GetValidatorParticipation(epoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving epoch participation statistics for epoch %v: %v", epoch, err)
	}

	t4 := time.Now()

	logger.WithFields(logrus.Fields{
		"validators":       len(data.Validators),
		"blocks":           len(data.Blocks),
		"dur":              time.Since(t0),
		"durParticipation": t4.Sub(t3),
		"durValidators":    t3.Sub(t2),
		"durBlocks":        t2.Sub(t1),
		"durAssignments":   t1.Sub(t0),
	}).Info("GetEpochData")

	return data, nil
}

func (c *HybridClient) GetValidatorQueue() (*types.ValidatorQueue, error) {
	return c.prysmClient.GetValidatorQueue()
}

func (c *HybridClient) GetAttestationPool() ([]*types.Attestation, error) {
	return c.prysmClient.GetAttestationPool()
}

func (c *HybridClient) GetEpochAssignments(epoch uint64) (*types.EpochAssignments, error) {
	return c.lhClient.GetEpochAssignments(epoch)
}

func (c *HybridClient) GetBlocksBySlot(slot uint64) ([]*types.Block, error) {
	return c.prysmClient.GetBlocksBySlot(slot)
}

func (c *HybridClient) GetValidatorParticipation(epoch uint64) (*types.ValidatorParticipation, error) {
	return c.prysmClient.GetValidatorParticipation(epoch)
}
