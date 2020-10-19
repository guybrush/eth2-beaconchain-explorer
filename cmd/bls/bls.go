/*

      block_ts       |                              tx_hash                               |                                             publickey                                              |                       withdrawal_credentials                       |   amount    |                                                                                             signature
---------------------+--------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+--------------------------------------------------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 2020-09-21 20:45:31 | \x34f6d383e2c041f318351498b111ab3bb27399afe2fea617d75f08d41ac8a078 | \xb5046818fc2fe6d91eb0b8af754a9bfbe7344a8b01c6113b7fb94390d02e0096ff8b2e287b347fe1635784226321bc6c | \x00d6ff0a6749d0ad12facc3f9c57c1b1b3bdd1bda87d3bff2f073036db53d7ff | 32000000000 | \x85295b97b4961967191555c9c80e2316d6d9606fd249e480f7ec0a16823f3025b853518a6ec57ec010e532e648c0415709deb7bf3090415f972d1dbd8362f38fe6d3dd01c8e3272a207e0c3ddf02041f1003b255e5690a7088985c5e89003fb1
 2020-09-21 20:44:31 | \xfef5f91ea08909740e013cb69c3600118020abbbaeca2782ba9a2382d9bd7495 | \xad8238cb360b2bee22fd38ea432c910b7f838610f9ef6b71d3d5475f5e676c97265a60135af4d0e051cc8408dd641f14 | \x008ffcb22647fb5fef23410e83534846df2d67d2d1d9918e35069919f9b82b50 | 32000000000 | \xb3c40ab265f53da3d4f9d49b4254953a4676177adeb57c41241feb1e04788f7926ea40bdd8169c50279e999dbdbcdd2418bb7d214b97fbcab7188d6c77975fa9f6ee3429852288c9649c1a23c404a6c689312e796bb0abea29cb31f5f247854e
 2020-09-21 20:46:16 | \xac44e56ff5f6baf1adb1a8e5337813a48e36fa89719a2de32f6720556389c24c | \x8c6112aaec8b7b293d45c3c361ea2df9a47402ffc05ebb184a655eb7b9e6e7c2424de2ad69340f94d21fab054c53f411 | \x00b7c6f91e0a221b081dd99a2fa9fef362ac9cbe5c9fe5238b4f2b0be6a719cb | 32000000000 | \x8156f225cc97f74213ac30706918f9666b08a6a66556ce8c334c92712bbe5fc63d9bcc1c472eea3daa2249616ab0c23b095c5967c7ace70af252ec937b2345bc64dafd2548556caa78dc94aec47c850bdfb637a7a255bcc9cbb3de89be01ceff
 2020-09-23 07:14:58 | \x9f6134df47fd606bc8f71c1a2d9cb29457780ca2d6033e7cbf9e804dde047e36 | \xa7f6dda4234960c33cba4ec57f0f34a7acd5eafd038a8659e7e96c9cb1839cbe23e3a21c87dd5e8edb94eff899a5b1e4 | \x00f4a31f799dfdd5546fce92ce4904ed5c2181ee6b6ac67597c29193776cc757 | 32000000000 | \x92cc99435df695e3d4700e62a1b921f05c76b858be8409d8a174b9db3949d4a2d0e38581ba22eb9618fad8a165b3a72e14f829c44db85fbdeac051d70847f0c035c7e744d1920f402c1f02873ecb9ba331d1e2da91aec40121626bfb5dbe6e7c
 2020-09-23 07:14:58 | \xbe48a6a1a73eb6c6053a33bed48855e61aeeae67d6ba5dd5a305fad495d66132 | \x8bb0fe7e28bff4c4f60007517679518a19652ac4ea79d0797738e36a372dda3684f809a2d6ab71c0c3e3bbb6b9fdfe8b | \x0011bd231d5bb65ea9f254d339da70f5d378ee142f2a89bc42bab44b9aeaeaf8 | 32000000000 | \xa45880d8dae3ce5c9459c663c7b03d7385472f14a394cb353c0d8fcf53a932d233adb5bd17ec415cdc8d5184e3ef4de10d4dd3d7fa150143dda9a63c6522e55c6eac29fd12fe746367b406bbe743a7ac2839d7cb1c13e3d50905ce266aab3ca8
 2020-09-23 07:15:28 | \xad9599b33c88c61bf96ac5f6926089df1b8295cb22cca3f9e5204686c9026095 | \xa975b25d2db306cbaa3a97785da4885ea71b4bfa39eaf8479ab76dbeb5b1e3d4f0abe7c7492329ea80c7e08b4f678ded | \x00f914d7904daa298d1a4fdc68e7262e6045a636bc9bdfe46bf50b1470c0a4f8 | 32000000000 | \xa18122aacc22d8095dcd5f01beb3bbbb13a0a93b12bdbcbcfd2f9e5c4d1657802927c963b1d87959b5132468d08add460aac430169185703468c00632c627bf4e064d7df636b5aeb3a31bd62a9f9beff74f5768a19fb01b6f6570a7fd8072197
(6 rows)

*/

package main

import (
	"encoding/hex"
	"fmt"

	ethpb "github.com/prysmaticlabs/ethereumapis/eth/v1alpha1"
	"github.com/prysmaticlabs/go-ssz"
	pb "github.com/prysmaticlabs/prysm/proto/beacon/p2p/v1"
	"github.com/prysmaticlabs/prysm/shared/bls"
	"github.com/prysmaticlabs/prysm/shared/params"
)

func main() {
	dd := &ethpb.Deposit_Data{}
	dd.PublicKey = MustHexDecodeString("b5046818fc2fe6d91eb0b8af754a9bfbe7344a8b01c6113b7fb94390d02e0096ff8b2e287b347fe1635784226321bc6c")
	dd.WithdrawalCredentials = MustHexDecodeString("00d6ff0a6749d0ad12facc3f9c57c1b1b3bdd1bda87d3bff2f073036db53d7ff")
	dd.Amount = uint64(32000000000)
	dd.Signature = MustHexDecodeString("85295b97b4961967191555c9c80e2316d6d9606fd249e480f7ec0a16823f3025b853518a6ec57ec010e532e648c0415709deb7bf3090415f972d1dbd8362f38fe6d3dd01c8e3272a207e0c3ddf02041f1003b255e5690a7088985c5e89003fb1")

	// dd.PublicKey = MustHexDecodeString("a975b25d2db306cbaa3a97785da4885ea71b4bfa39eaf8479ab76dbeb5b1e3d4f0abe7c7492329ea80c7e08b4f678ded")
	// dd.WithdrawalCredentials = MustHexDecodeString("00f914d7904daa298d1a4fdc68e7262e6045a636bc9bdfe46bf50b1470c0a4f8")
	// dd.Amount = 32000000000
	// dd.Signature = MustHexDecodeString("a18122aacc22d8095dcd5f01beb3bbbb13a0a93b12bdbcbcfd2f9e5c4d1657802927c963b1d87959b5132468d08add460aac430169185703468c00632c627bf4e064d7df636b5aeb3a31bd62a9f9beff74f5768a19fb01b6f6570a7fd8072197")

	err := VerifyEth1DepositSignature(dd, "spadina")
	fmt.Println(err)
}

func MustHexDecodeString(in string) []byte {
	out, _ := hex.DecodeString(in)
	return out
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

func VerifyEth1DepositSignature(obj *ethpb.Deposit_Data, network string) error {
	cfg := params.BeaconConfig()
	if network == "altona" {
		cfg = params.AltonaConfig()
	} else if network == "medalla" {
		cfg = params.MedallaConfig()
	} else if network == "spadina" {
		cfg = params.SpadinaConfig()
	}
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
