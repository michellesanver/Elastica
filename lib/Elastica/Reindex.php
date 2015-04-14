<?php

namespace Elastica;

use Elastica\Document;
use Elastica\ScanAndScroll;
use Elastica\Client;
use Elastica\Index;
use Elastica\Search;
use Elastica\Exception\InvalidException;

/**
 * Elastica reindex
 *
 * @category Xodoa
 * @package Elastica
 * @author Michelle Sanver <michelle.sanver@liip.ch>
 */
class Reindex
{
    /**
     * @var Client
     */
    private $client;


    /**
     * Constructor
     *
     * @param Client $client The Elastica client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }


    /**
     * Reindex one index from source to target
     *
     * @param Index $sourceIndex The source index
     * @param Index $targetIndex The target
     * @param array $options     Options are: indexSettings, expiryTime, sizePerShard
     *
     * @return void
     */
    public function reindex(Index $sourceIndex, Index $targetIndex, array $options = array())
    {
        $expiryTime = "1m";
        $sizePerShard = "1000";
        $indexSettings = null;

        if (isset($options['index_settings'])) {
            $indexSettings = $options['index_settings'];
        }

        if (isset($options['expiry_time'])) {
            $expiryTime = $options['expiry_time'];
        }

        if (isset($options['size_per_shard'])) {
            $expiryTime = $options['size_per_shard'];
        }

        $this->setIndexSettings($sourceIndex, $indexSettings);
        $this->setIndexSettings($targetIndex, $indexSettings);

        $search = new Search($this->client);
        $search->addIndex($sourceIndex);
        $search->setQuery('*');

        foreach ($search->scanAndScroll($expiryTime, $sizePerShard) as $scrollId => $resultSet) {
            $results = $resultSet->getResults();
            $documents = array();

            foreach ($results as $result) {
                $hit = $result->getHit();
                $document = new Document($hit['_id'], $hit['_source'], $hit['_type'], $hit['_index']);
                $documents[] = $document;
            }

            $targetIndex->addDocuments($documents);
            $targetIndex->refresh();
        }
    }

    /**
     * Set the index settings, bulk indexing if not otherwise specified
     *
     * @param Index      $index         The index to set the settings on
     * @param null|array $indexSettings The settings to set
     *
     * @return void
     */
    private function setIndexSettings(Index $index, $indexSettings = null)
    {
        $bulkIndexSettings = array(
            'index' => array(
                'refresh_interval' => '-1',
                'merge.policy.merge_factor' => 30
            )
        );

        if (null == $indexSettings) {
            $indexSettings = $bulkIndexSettings;
        }

        $index->setSettings($indexSettings);
    }
}
