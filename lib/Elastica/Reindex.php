<?php

namespace Elastica;

use Elastica\Document;
use Elastica\ScanAndScroll;
use Elastica\Client;
use Elastica\Index;
use Elastica\Search;
use Elastica\Exception\InvalidException;
use Elastica\ResultSet;

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
        $expiryTime = (isset($options['expiry_time']) ? $options['expiry_time'] : '1m');
        $sizePerShard = (isset($options['size_per_shard']) ? $options['size_per_shard'] : 1000);
        $indexSettings = (isset($options['index_settings']) ? $options['index_settings'] : null);

        $this->setIndexSettings(array($sourceIndex, $targetIndex), $indexSettings);

        $search = new Search($this->client);
        $search->addIndex($sourceIndex);
        $search->setQuery('*');

        foreach ($search->scanAndScroll($expiryTime, $sizePerShard) as $scrollId => $resultSet) {
            $results = $resultSet->getResults();
            $this->addDocumentsToIndex($results, $targetIndex);
        }
    }

    /**
     * Add results to specified target index
     *
     * @param ResultSet $results     The results to add to the target
     * @param Index     $targetIndex The target index we want to add the results to
     *
     * @return void
     */
    private function addDocumentsToIndex(ResultSet $results, Index $targetIndex)
    {
        $documents = array();

        foreach ($results as $result) {
            $hit = $result->getHit();
            $documents[] = new Document($hit['_id'], $hit['_source'], $hit['_type'], $hit['_index']);
        }

        $targetIndex->addDocuments($documents);
        $targetIndex->refresh();
    }

    /**
     * Set the index settings, bulk indexing if not otherwise specified
     *
     * @param array      $indexes       The indexes to set the settings on
     * @param null|array $indexSettings The settings to set
     *
     * @return void
     */
    private function setIndexSettings(array $indexes, $indexSettings = null)
    {
        foreach ($indexes as $index) {
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
}
