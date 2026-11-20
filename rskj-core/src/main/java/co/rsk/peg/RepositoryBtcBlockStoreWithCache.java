/*
 * This file is part of RskJ
 * Copyright (C) 2017 RSK Labs Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package co.rsk.peg;

import static co.rsk.bitcoinj.core.StoredBlock.deserializeCompactLegacy;
import static co.rsk.bitcoinj.core.StoredBlock.deserializeCompactV2;

import co.rsk.bitcoinj.core.BtcBlock;
import co.rsk.bitcoinj.core.NetworkParameters;
import co.rsk.bitcoinj.core.Sha256Hash;
import co.rsk.bitcoinj.core.StoredBlock;
import co.rsk.bitcoinj.store.BlockStoreException;
import co.rsk.peg.constants.BridgeConstants;
import co.rsk.core.RskAddress;
import co.rsk.util.MaxSizeHashMap;
import java.util.Optional;
import com.google.common.annotations.VisibleForTesting;
import org.ethereum.config.blockchain.upgrades.ActivationConfig;
import org.ethereum.config.blockchain.upgrades.ConsensusRule;
import org.ethereum.config.blockchain.upgrades.ActivationConfig.ForBlock;
import org.ethereum.core.Repository;
import org.ethereum.vm.DataWord;
import org.ethereum.vm.PrecompiledContracts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Implementation of a bitcoinj blockstore that persists to RSK's Repository
 *
 * The cache is DEMAND-POPULATED and holds NO NEGATIVE ENTRIES.
 *
 *  - Nothing is read speculatively. Entries come from reads the node performed
 *    because something asked for them, and from put(), which involves no read.
 *    This keeps a block's repository read set equal to what consensus required,
 *    which state proofs and snapshot-style sync depend on.
 *
 *  - get() populates the cache but never answers from it. One Factory's cache
 *    is shared by every store it makes, and those stores have separate
 *    repository tracks, so answering from the cache would let one track report
 *    a block present that only another track holds. getFromCache() is the
 *    cache accessor; getStoredBlockAtMainChainDepth consults it first and falls
 *    back to get(), so nothing on the consensus path loses a cache hit.
 *
 *  - An absent block is never recorded. A repository read that cannot find a
 *    node and a genuinely absent value are the same null, so caching an absence
 *    would let a partial state produce a wrong answer for a later
 *    consensus-required lookup. The cache may answer or say nothing; it must
 *    never say "no".
 *
 * Only hash -> StoredBlock is cached. The chain head and the RSKIP199
 * height->hash index are mutable and are deliberately left uncached.
 *
 * @author Oscar Guindzberg
 */
public class RepositoryBtcBlockStoreWithCache implements BtcBlockStoreWithCache {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryBtcBlockStoreWithCache.class);

    private static final String BLOCK_STORE_CHAIN_HEAD_KEY = "blockStoreChainHead";
    private static final int DEFAULT_MAX_DEPTH_BLOCK_CACHE = 5_000;
    private static final int DEFAULT_MAX_SIZE_BLOCK_CACHE = 10_000;

    private final Repository repository;
    private final RskAddress contractAddress;
    private final NetworkParameters btcNetworkParams;
    private final BridgeConstants bridgeConstants;
    private final BridgeStorageProvider bridgeStorageProvider;
    private final ActivationConfig.ForBlock activations;
    private final int maxDepthBlockCache;
    private final Map<Sha256Hash, StoredBlock> cacheBlocks;

    public RepositoryBtcBlockStoreWithCache(
        NetworkParameters btcNetworkParams,
        Repository repository,
        Map<Sha256Hash, StoredBlock> cacheBlocks,
        RskAddress contractAddress,
        BridgeConstants bridgeConstants,
        BridgeStorageProvider bridgeStorageProvider,
        ForBlock activations) {

        this(
            btcNetworkParams,
            repository,
            cacheBlocks,
            contractAddress,
            bridgeConstants,
            bridgeStorageProvider,
            activations,
            DEFAULT_MAX_DEPTH_BLOCK_CACHE
        );
    }

    public RepositoryBtcBlockStoreWithCache(
        NetworkParameters btcNetworkParams,
        Repository repository,
        Map<Sha256Hash, StoredBlock> cacheBlocks,
        RskAddress contractAddress,
        BridgeConstants bridgeConstants,
        BridgeStorageProvider bridgeStorageProvider,
        ForBlock activations,
        int maxDepthBlockCache) {

        this.cacheBlocks = cacheBlocks;
        this.repository = repository;
        this.contractAddress = contractAddress;
        this.btcNetworkParams = btcNetworkParams;
        this.bridgeConstants = bridgeConstants;
        this.bridgeStorageProvider = bridgeStorageProvider;
        this.activations = activations;
        this.maxDepthBlockCache = maxDepthBlockCache;

        checkIfInitialized();
    }

    @Override
    public synchronized void put(StoredBlock storedBlock) {
        Sha256Hash hash = storedBlock.getHeader().getHash();
        byte[] ba = storedBlockToByteArray(storedBlock);
        repository.addStorageBytes(contractAddress, DataWord.valueFromHex(hash.toString()), ba);
        if (cacheBlocks != null) {
            StoredBlock chainHead = getChainHead();
            // Caching a block we are writing costs no read. The depth gate is a
            // slot-budget heuristic only; since populateCache was removed it no
            // longer bounds any speculative walk. Blocks below this depth still
            // reach the cache when a lookup actually reads them (see get()).
            if (chainHead == null || chainHead.getHeight() - storedBlock.getHeight() < this.maxDepthBlockCache) {
                cacheBlocks.put(storedBlock.getHeader().getHash(), storedBlock);
            }
        }
    }

    @Override
    public synchronized StoredBlock get(Sha256Hash hash) {
        // Deliberately does NOT answer from the cache. get() reports what this
        // instance's repository holds, and callers rely on that: the cache is
        // shared by every store a Factory makes, while each store has its own
        // repository track, so serving from it here would let one track report a
        // block that only exists in another -- a presence leak across forks.
        // getFromCache() is the cache accessor, and
        // getStoredBlockAtMainChainDepth already consults it before falling
        // back here, so the consensus path loses no hits.
        logger.trace("[get] Looking in storage for block with hash {}", hash);
        byte[] ba = repository.getStorageBytes(contractAddress, DataWord.valueFromHex(hash.toString()));
        if (ba == null) {
            logger.trace("[get] Block with hash {} not found in storage", hash);
            // Deliberately NOT cached. A repository read that cannot find a node
            // and a genuinely absent value both return null, and nothing at this
            // layer can tell them apart. Recording an absence would let a partial
            // state turn into a wrong answer for a later consensus-required
            // lookup. See the class javadoc.
            return null;
        }

        StoredBlock block = byteArrayToStoredBlock(ba);
        if (cacheBlocks != null) {
            // Populating from a read that happened anyway is the whole of R1.
            // Safe to share across tracks: the entry is keyed by the block's own
            // header hash and a StoredBlock's height and chain work are fixed by
            // its ancestry, so the value cannot be wrong or go stale. Only
            // *presence* is track-specific, which is why get() does not answer
            // from here.
            cacheBlocks.put(hash, block);
        }
        return block;
    }

    @Override
    public synchronized StoredBlock getChainHead() {
        byte[] ba = repository.getStorageBytes(contractAddress, DataWord.fromString(BLOCK_STORE_CHAIN_HEAD_KEY));
        if (ba == null) {
            return null;
        }
        return byteArrayToStoredBlock(ba);
    }

    @Override
    public synchronized void setChainHead(StoredBlock newChainHead) {
        logger.trace("Set new chain head with height: {}.", newChainHead.getHeight());
        byte[] ba = storedBlockToByteArray(newChainHead);
        repository.addStorageBytes(contractAddress, DataWord.fromString(BLOCK_STORE_CHAIN_HEAD_KEY), ba);
        setMainChainBlock(newChainHead.getHeight(), newChainHead.getHeader().getHash());
    }

    @Override
    public Optional<StoredBlock> getInMainchain(int height) {
        Optional<Sha256Hash> bestBlockHash = bridgeStorageProvider.getBtcBestBlockHashByHeight(height);
        if (!bestBlockHash.isPresent()) {
            logger.trace("[getInMainchain] Block at height {} not present in storage", height);
            return Optional.empty();
        }

        StoredBlock block = get(bestBlockHash.get());
        if (block == null) {
            logger.trace("[getInMainchain] Block with hash {} not found in storage", bestBlockHash.get());
            return Optional.empty();
        }

        logger.trace("[getInMainchain] Found block with hash {} at height {}", bestBlockHash.get(), height);
        return Optional.of(block);
    }

    @Override
    public void setMainChainBlock(int height, Sha256Hash blockHash) {
        logger.trace("[setMainChainBlock] Set block with hash {} at height {}", blockHash, height);
        bridgeStorageProvider.setBtcBestBlockHashByHeight(height, blockHash);
    }

    @Override
    public void close() {
    }

    @Override
    public NetworkParameters getParams() {
        return btcNetworkParams;
    }

    @Override
    public StoredBlock getFromCache(Sha256Hash branchBlockHash) {
        if (cacheBlocks == null) {
            logger.trace("[getFromCache] Block with hash {} not found in cache", branchBlockHash);
            return null;
        }
        return cacheBlocks.get(branchBlockHash);
    }

    @Override
    public StoredBlock getStoredBlockAtMainChainHeight(int height) throws BlockStoreException {
        StoredBlock chainHead = getChainHead();
        int depth = chainHead.getHeight() - height;
        logger.trace("Getting btc block at depth: {}", depth);

        if (depth < 0) {
            String message = String.format(
                "Height provided is higher than chain head. provided: %s. chain head: %s",
                height,
                chainHead.getHeight()
            );
            logger.trace("[getStoredBlockAtMainChainHeight] {}", message);
            throw new BlockStoreException(message);
        }

        if (activations.isActive(ConsensusRule.RSKIP199)) {
            int btcHeightWhenBlockIndexActivates = this.bridgeConstants.getBtcHeightWhenBlockIndexActivates();
            int maxDepthToSearch = this.bridgeConstants.getMaxDepthToSearchBlocksBelowIndexActivation();
            int limit;
            if (chainHead.getHeight() - btcHeightWhenBlockIndexActivates > maxDepthToSearch) {
                limit = btcHeightWhenBlockIndexActivates;
            } else {
                limit = chainHead.getHeight() - maxDepthToSearch;
            }
            logger.trace("[getStoredBlockAtMainChainHeight] Chain head height is {} and the depth limit {}", chainHead.getHeight(), limit);

            if (height < limit) {
                String message = String.format(
                    "Height provided is lower than the depth limit defined to search for blocks. Provided: %n, limit: %n",
                    height,
                    limit
                );
                logger.trace("[getStoredBlockAtMainChainHeight] {}", message);
                throw new BlockStoreException(message);
            }
        }

        StoredBlock block;
        Optional<StoredBlock> blockOptional = getInMainchain(height);
        if (blockOptional.isPresent()) {
            block = blockOptional.get();
        } else {
            block = getStoredBlockAtMainChainDepth(depth);
        }

        return block;
    }


    @Override
    @Deprecated
    public StoredBlock getStoredBlockAtMainChainDepth(int depth) throws BlockStoreException {
        logger.trace("[getStoredBlockAtMainChainDepth] Looking for block at depth {}", depth);
        StoredBlock chainHead = getChainHead();
        Sha256Hash blockHash = chainHead.getHeader().getHash();

        for (int i = 0; i < depth && blockHash != null; i++) {
            //If its older than cache go to disk
            StoredBlock currentBlock = getFromCache(blockHash);
            if (currentBlock == null) {
                logger.trace("[getStoredBlockAtMainChainDepth] Block with hash {} not in cache, getting from disk", blockHash);
                currentBlock = get(blockHash);
                if (currentBlock == null) {
                    return null;
                }
            }
            blockHash = currentBlock.getHeader().getPrevBlockHash();
        }

        if (blockHash == null) {
            logger.trace("[getStoredBlockAtMainChainDepth] Block not found");
            return null;
        }
        StoredBlock block = getFromCache(blockHash);
        if (block == null) {
            block = get(blockHash);
        }
        int expectedHeight = chainHead.getHeight() - depth;
        if (block != null && block.getHeight() != expectedHeight) {
            String message = String.format("Block %s at depth %d Height is %d but should be %d",
                block.getHeader().getHash(),
                depth,
                block.getHeight(),
                expectedHeight
            );
            logger.trace("[getStoredBlockAtMainChainDepth] {}", message);
            throw new BlockStoreException(message);
        }

        return block;
    }

    private byte[] storedBlockToByteArray(StoredBlock block) {
        ByteBuffer byteBuffer = serializeBlock(block);
        byte[] ba = new byte[byteBuffer.position()];
        byteBuffer.flip();
        byteBuffer.get(ba);
        return ba;
    }

    private ByteBuffer serializeBlock(StoredBlock block) {
        if (shouldUseLegacy12ByteChainworkFormat()) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(StoredBlock.COMPACT_SERIALIZED_SIZE_LEGACY);
            block.serializeCompactLegacy(byteBuffer);
            return byteBuffer;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(StoredBlock.COMPACT_SERIALIZED_SIZE_V2);
        block.serializeCompactV2(byteBuffer);
        return byteBuffer;
    }

    private boolean shouldUseLegacy12ByteChainworkFormat() {
        return !activations.isActive(ConsensusRule.RSKIP454);
    }

    private StoredBlock byteArrayToStoredBlock(byte[] ba) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(ba);
        if (ba.length == StoredBlock.COMPACT_SERIALIZED_SIZE_LEGACY) {
            return deserializeCompactLegacy(btcNetworkParams, byteBuffer);
        }

        return deserializeCompactV2(btcNetworkParams, byteBuffer);
    }

    private void checkIfInitialized() {
        if (getChainHead() == null) {
            BtcBlock genesisHeader = this.btcNetworkParams.getGenesisBlock().cloneAsHeader();
            StoredBlock storedGenesis = new StoredBlock(genesisHeader, genesisHeader.getWork(), 0);
            put(storedGenesis);
            setChainHead(storedGenesis);
        }
    }

    public static class Factory implements BtcBlockStoreWithCache.Factory {

        private final int maxSizeBlockCache;
        //This is ok as we don't have parallel execution, in the feature we should move to a concurrentHashMap
        private final Map<Sha256Hash, StoredBlock> cacheBlocks;
        private final RskAddress contractAddress;
        private final NetworkParameters btcNetworkParams;
        private final int maxDepthBlockCache;

        @VisibleForTesting
        public Factory(NetworkParameters btcNetworkParams) {
            this(btcNetworkParams, DEFAULT_MAX_DEPTH_BLOCK_CACHE, DEFAULT_MAX_SIZE_BLOCK_CACHE);
        }

        public Factory(NetworkParameters btcNetworkParams, int maxDepthBlockCache, int maxSizeBlockCache) {
            this.contractAddress = PrecompiledContracts.BRIDGE_ADDR;
            this.btcNetworkParams = btcNetworkParams;
            this.maxDepthBlockCache = maxDepthBlockCache;
            this.maxSizeBlockCache = maxSizeBlockCache;
            this.cacheBlocks = new MaxSizeHashMap<>(this.maxSizeBlockCache, true);

            if (this.maxDepthBlockCache > this.maxSizeBlockCache) {
                logger.warn("Max depth ({}) is greater than Max Size ({}). This could lead to a misbehaviour.", this.maxDepthBlockCache, this.maxSizeBlockCache);
            }

            if (this.maxDepthBlockCache < DEFAULT_MAX_DEPTH_BLOCK_CACHE) {
                logger.warn("Max depth ({}) is lower than the default ({}). This could lead to a misbehaviour.", this.maxDepthBlockCache, DEFAULT_MAX_DEPTH_BLOCK_CACHE);
            }
        }

        @Override
        public BtcBlockStoreWithCache newInstance(
            Repository track,
            BridgeConstants bridgeConstants,
            BridgeStorageProvider bridgeStorageProvider,
            ActivationConfig.ForBlock activations) {

            return new RepositoryBtcBlockStoreWithCache(
                btcNetworkParams,
                track,
                cacheBlocks,
                contractAddress,
                bridgeConstants,
                bridgeStorageProvider,
                activations,
                this.maxDepthBlockCache
            );
        }
    }
}
