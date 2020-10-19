package main

import (
	"context"
	"eth2-exporter/types"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethRPC "github.com/ethereum/go-ethereum/rpc"
	ethpb "github.com/prysmaticlabs/ethereumapis/eth/v1alpha1"
	"github.com/prysmaticlabs/go-ssz"
	contracts "github.com/prysmaticlabs/prysm/contracts/deposit-contract"
	pb "github.com/prysmaticlabs/prysm/proto/beacon/p2p/v1"
	"github.com/prysmaticlabs/prysm/shared/bls"
	"github.com/prysmaticlabs/prysm/shared/bytesutil"
	"github.com/prysmaticlabs/prysm/shared/hashutil"
	"github.com/prysmaticlabs/prysm/shared/params"
	"github.com/sirupsen/logrus"
)

var eth1LookBack = uint64(100)
var eth1MaxFetch = uint64(1000)
var eth1DepositEventSignature = hashutil.HashKeccak256([]byte("DepositEvent(bytes,bytes,bytes,bytes,bytes)"))
var eth1DepositContractFirstBlock uint64
var eth1DepositContractAddress common.Address
var eth1Client *ethclient.Client
var eth1RPCClient *gethRPC.Client

var logger = logrus.New().WithField("module", "exporter")

func main() {
	eth1DepositsExporter()
}

// eth1DepositsExporter regularly fetches the depositcontract-logs of the
// last 100 blocks and exports the deposits into the database.
// If a reorg of the eth1-chain happened within these 100 blocks it will delete
// removed deposits.
func eth1DepositsExporter() {
	eth1DepositContractAddress = common.HexToAddress("0x48B597F4b53C21B48AD95c7256B49D1779Bd5890")
	eth1DepositContractFirstBlock = 3384340

	rpcClient, err := gethRPC.Dial("https://goerli.infura.io/v3/730f61d0cad749e6905d44aca4e3f44c")
	if err != nil {
		logger.Fatal(err)
	}
	eth1RPCClient = rpcClient
	client := ethclient.NewClient(rpcClient)
	eth1Client = client

	depositsToSave, err := fetchEth1Deposits(3448162, 3448162)
	if err != nil {
		fmt.Println("error", err)
		return
	}
	for _, d := range depositsToSave {
		fmt.Printf(`
txhash  : %x
txidx   : %x
block   : %v
from    : %x
pubk    : %x
withdrw : %x
amt     : %v
sig     : %x
mrkltidx: %x
removed : %v
valid   : %v`+"\n",
			d.TxHash, d.TxIndex, d.BlockNumber, d.FromAddress, d.PublicKey, d.WithdrawalCredentials,
			d.Amount, d.Signature, d.MerkletreeIndex, d.Removed, d.ValidSignature,
		)
	}

}

func fetchEth1Deposits(fromBlock, toBlock uint64) (depositsToSave []*types.Eth1Deposit, err error) {
	qry := ethereum.FilterQuery{
		Addresses: []common.Address{
			eth1DepositContractAddress,
		},
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
	}

	depositLogs, err := eth1Client.FilterLogs(context.Background(), qry)
	if err != nil {
		return depositsToSave, fmt.Errorf("error getting logs from eth1-client: %w", err)
	}

	blocksToFetch := []uint64{}
	txsToFetch := []string{}

	for _, depositLog := range depositLogs {
		if depositLog.Topics[0] != eth1DepositEventSignature {
			continue
		}
		pubkey, withdrawalCredentials, amount, signature, merkletreeIndex, err := contracts.UnpackDepositLogData(depositLog.Data)
		if err != nil {
			return depositsToSave, fmt.Errorf("error unpacking eth1-deposit-log: %x: %w", depositLog.Data, err)
		}
		fmt.Printf(`
txhash: %x
merkid: %x
pubkey: %x
withdc: %x
amount: %v
signat: %x
`, depositLog.TxHash.Bytes(), merkletreeIndex, pubkey, withdrawalCredentials, bytesutil.FromBytes8(amount), signature)
		err = VerifyEth1DepositSignature(&ethpb.Deposit_Data{
			PublicKey:             pubkey,
			WithdrawalCredentials: withdrawalCredentials,
			Amount:                bytesutil.FromBytes8(amount),
			Signature:             signature,
		})
		validSignature := err == nil
		blocksToFetch = append(blocksToFetch, depositLog.BlockNumber)
		txsToFetch = append(txsToFetch, depositLog.TxHash.Hex())
		depositsToSave = append(depositsToSave, &types.Eth1Deposit{
			TxHash:                depositLog.TxHash.Bytes(),
			TxIndex:               uint64(depositLog.TxIndex),
			BlockNumber:           depositLog.BlockNumber,
			PublicKey:             pubkey,
			WithdrawalCredentials: withdrawalCredentials,
			Amount:                bytesutil.FromBytes8(amount),
			Signature:             signature,
			MerkletreeIndex:       merkletreeIndex,
			Removed:               depositLog.Removed,
			ValidSignature:        validSignature,
		})
	}

	headers, txs, err := eth1BatchRequestHeadersAndTxs(blocksToFetch, txsToFetch)
	if err != nil {
		return depositsToSave, fmt.Errorf("error getting eth1-blocks: %w", err)
	}

	for _, d := range depositsToSave {
		// get corresponding block (for the tx-time)
		b, exists := headers[d.BlockNumber]
		if !exists {
			return depositsToSave, fmt.Errorf("error getting block for eth1-deposit: block does not exist in fetched map")
		}
		d.BlockTs = int64(b.Time)

		// get corresponding tx (for input and from-address)
		tx, exists := txs[fmt.Sprintf("0x%x", d.TxHash)]
		if !exists {
			return depositsToSave, fmt.Errorf("error getting tx for eth1-deposit: tx does not exist in fetched map")
		}
		d.TxInput = tx.Data()
		chainID := tx.ChainId()
		if chainID == nil {
			return depositsToSave, fmt.Errorf("error getting tx-chainId for eth1-deposit")
		}
		signer := gethTypes.NewEIP155Signer(chainID)
		sender, err := signer.Sender(tx)
		if err != nil {
			return depositsToSave, fmt.Errorf("error getting sender for eth1-deposit")
		}
		d.FromAddress = sender.Bytes()
	}

	return depositsToSave, nil
}

// eth1BatchRequestHeadersAndTxs requests the block range specified in the arguments.
// Instead of requesting each block in one call, it batches all requests into a single rpc call.
// This code is shamelessly stolen and adapted from https://github.com/prysmaticlabs/prysm/blob/2eac24c/beacon-chain/powchain/service.go#L473
func eth1BatchRequestHeadersAndTxs(blocksToFetch []uint64, txsToFetch []string) (map[uint64]*gethTypes.Header, map[string]*gethTypes.Transaction, error) {
	elems := make([]gethRPC.BatchElem, 0, len(blocksToFetch)+len(txsToFetch))
	headers := make(map[uint64]*gethTypes.Header, len(blocksToFetch))
	txs := make(map[string]*gethTypes.Transaction, len(txsToFetch))
	errors := make([]error, 0, len(blocksToFetch)+len(txsToFetch))

	for _, b := range blocksToFetch {
		header := &gethTypes.Header{}
		err := error(nil)
		elems = append(elems, gethRPC.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{hexutil.EncodeBig(big.NewInt(int64(b))), false},
			Result: header,
			Error:  err,
		})
		headers[b] = header
		errors = append(errors, err)
	}

	for _, txHashHex := range txsToFetch {
		tx := &gethTypes.Transaction{}
		err := error(nil)
		elems = append(elems, gethRPC.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{txHashHex},
			Result: tx,
			Error:  err,
		})
		txs[txHashHex] = tx
		errors = append(errors, err)
	}

	ioErr := eth1RPCClient.BatchCall(elems)
	if ioErr != nil {
		return nil, nil, ioErr
	}

	for _, e := range errors {
		if e != nil {
			return nil, nil, e
		}
	}

	return headers, txs, nil
}

// From: "github.com/prysmaticlabs/prysm/beacon-chain/core/helpers"
// Avoid including dependency directly as it triggers a
// Cloudflare roughtime call that blocks startup for
// several seconds
// ForkVersionByteLength length of fork version byte array.
const ForkVersionByteLength = 4

// DomainByteLength length of domain byte array.
const DomainByteLength = 4

func ComputeDomain(domainType [DomainByteLength]byte, forkVersion []byte, genesisValidatorsRoot []byte) ([]byte, error) {
	if forkVersion == nil {
		forkVersion = params.BeaconConfig().GenesisForkVersion
	}
	if genesisValidatorsRoot == nil {
		genesisValidatorsRoot = params.BeaconConfig().ZeroHash[:]
	}
	forkBytes := [ForkVersionByteLength]byte{}
	copy(forkBytes[:], forkVersion)

	forkDataRoot, err := computeForkDataRoot(forkBytes[:], genesisValidatorsRoot)
	if err != nil {
		return nil, err
	}

	return domain(domainType, forkDataRoot[:]), nil
}

func domain(domainType [DomainByteLength]byte, forkDataRoot []byte) []byte {
	b := []byte{}
	b = append(b, domainType[:4]...)
	b = append(b, forkDataRoot[:28]...)
	return b
}

func computeForkDataRoot(version []byte, root []byte) ([32]byte, error) {
	r, err := ssz.HashTreeRoot(&pb.ForkData{
		CurrentVersion:        version,
		GenesisValidatorsRoot: root,
	})
	if err != nil {
		return [32]byte{}, err
	}
	return r, nil
}

func VerifyEth1DepositSignature(obj *ethpb.Deposit_Data) error {
	cfg := params.SpadinaConfig()
	domain, err := ComputeDomain(
		cfg.DomainDeposit,
		cfg.GenesisForkVersion,
		cfg.ZeroHash[:],
	)
	if err != nil {
		return fmt.Errorf("could not get domain: %w", err)
	}
	blsPubkey, err := bls.PublicKeyFromBytes(obj.PublicKey)
	if err != nil {
		return fmt.Errorf("could not get pubkey: %w", err)
	}
	blsSig, err := bls.SignatureFromBytes(obj.Signature)
	if err != nil {
		return fmt.Errorf("could not get sig %w", err)
	}
	root, err := ssz.SigningRoot(obj)
	if err != nil {
		return fmt.Errorf("could not get root: %w", err)
	}
	signingData := &pb.SigningData{
		ObjectRoot: root[:],
		Domain:     domain,
	}
	ctrRoot, err := ssz.HashTreeRoot(signingData)
	if err != nil {
		return fmt.Errorf("could not get ctr root: %w", err)
	}
	if !blsSig.Verify(blsPubkey, ctrRoot[:]) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}
