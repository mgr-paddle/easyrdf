<?php

namespace EasyRdf\Sparql;

/*
 * EasyRdf
 *
 * LICENSE
 *
 * Copyright (c) 2009-2015 Nicholas J Humfrey.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. The name of the author 'Nicholas J Humfrey" may be used to endorse or
 *    promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @package    EasyRdf
 * @copyright  Copyright (c) 2009-2015 Nicholas J Humfrey
 * @license    https://www.opensource.org/licenses/bsd-license.php
 */
use EasyRdf\Exception;
use EasyRdf\Format;
use EasyRdf\Graph;
use EasyRdf\Http;
use EasyRdf\RdfNamespace;
use EasyRdf\Utils;

/**
 * Class for making SPARQL queries using the SPARQL 1.1 Protocol
 *
 * @copyright  Copyright (c) 2009-2015 Nicholas J Humfrey
 * @license    https://www.opensource.org/licenses/bsd-license.php
 */
class Client
{
    /** The query/read address of the SPARQL Endpoint */
    private $queryUri;

    private $queryUri_has_params = false;

    /** The update/write address of the SPARQL Endpoint */
    private $updateUri;

    /** Create a new SPARQL endpoint client
     *
     * If the query and update endpoints are the same, then you
     * only need to give a single URI.
     *
     * @param string $queryUri  The address of the SPARQL Query Endpoint
     * @param string $updateUri Optional address of the SPARQL Update Endpoint
     */
    public function __construct($queryUri, $updateUri = null)
    {
        $this->queryUri = $queryUri;

        $parseUrlResult = parse_url($queryUri, \PHP_URL_QUERY) ?? '';

        if ('' !== $parseUrlResult) {
            $this->queryUri_has_params = true;
        } else {
            $this->queryUri_has_params = false;
        }

        if ($updateUri) {
            $this->updateUri = $updateUri;
        } else {
            $this->updateUri = $queryUri;
        }
    }

    /** Get the URI of the SPARQL query endpoint
     *
     * @return string The query URI of the SPARQL endpoint
     */
    public function getQueryUri()
    {
        return $this->queryUri;
    }

    /** Get the URI of the SPARQL update endpoint
     *
     * @return string The query URI of the SPARQL endpoint
     */
    public function getUpdateUri()
    {
        return $this->updateUri;
    }

    /**
     * @depredated
     *
     * @ignore
     */
    public function getUri()
    {
        return $this->queryUri;
    }

    /** Make a query to the SPARQL endpoint
     *
     * SELECT and ASK queries will return an object of type
     * EasyRdf\Sparql\Result.
     *
     * CONSTRUCT and DESCRIBE queries will return an object
     * of type EasyRdf\Graph.
     *
     * @param string $query The query string to be executed
     *
     * @return Result|\EasyRdf\Graph result of the query
     */
    public function query($query)
    {
        return $this->request('query', $query);
    }

    /** Count the number of triples in a SPARQL 1.1 endpoint
     *
     * Performs a SELECT query to estriblish the total number of triples.
     *
     * Counts total number of triples by default but a conditional triple pattern
     * can be given to count of a subset of all triples.
     *
     * @param string $condition Triple-pattern condition for the count query
     *
     * @return int The number of triples
     */
    public function countTriples($condition = '?s ?p ?o')
    {
        // SELECT (COUNT(*) AS ?count)
        // WHERE {
        //   {?s ?p ?o}
        //   UNION
        //   {GRAPH ?g {?s ?p ?o}}
        // }
        $result = $this->query('SELECT (COUNT(*) AS ?count) {'.$condition.'}');

        return $result[0]->count->getValue();
    }

    /** Get a list of named graphs from a SPARQL 1.1 endpoint
     *
     * Performs a SELECT query to get a list of the named graphs
     *
     * @param string $limit Optional limit to the number of results
     *
     * @return \EasyRdf\Resource[] array of objects for each named graph
     */
    public function listNamedGraphs($limit = null)
    {
        $query = 'SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o}}';
        if (null !== $limit) {
            $query .= ' LIMIT '.(int) $limit;
        }
        $result = $this->query($query);

        // Convert the result object into an array of resources
        $graphs = [];
        foreach ($result as $row) {
            $graphs[] = $row->g;
        }

        return $graphs;
    }

    /** Make an update request to the SPARQL endpoint
     *
     * Successful responses will return the HTTP response object
     *
     * Unsuccessful responses will throw an exception
     *
     * @param string $query The update query string to be executed
     *
     * @return Http\Response|\GuzzleHttp\Psr7\Response HTTP response
     */
    public function update($query)
    {
        return $this->request('update', $query);
    }

    public function insert($data, $graphUri = null)
    {
        // $this->updateData('INSET',
        $query = 'INSERT DATA {';
        if ($graphUri) {
            $query .= "GRAPH <$graphUri> {";
        }
        $query .= $this->convertToTriples($data);
        if ($graphUri) {
            $query .= '}';
        }
        $query .= '}';

        return $this->update($query);
    }

    protected function updateData($operation, $data, $graphUri = null)
    {
        $query = "$operation DATA {";
        if ($graphUri) {
            $query .= "GRAPH <$graphUri> {";
        }
        $query .= $this->convertToTriples($data);
        if ($graphUri) {
            $query .= '}';
        }
        $query .= '}';

        return $this->update($query);
    }

    public function clear($graphUri, $silent = false)
    {
        $query = 'CLEAR';
        if ($silent) {
            $query .= ' SILENT';
        }
        if (preg_match('/^all|named|default$/i', $graphUri)) {
            $query .= " $graphUri";
        } else {
            $query .= " GRAPH <$graphUri>";
        }

        return $this->update($query);
    }

    /*
     * Internal function to make an HTTP request to SPARQL endpoint
     *
     * @ignore
     */
    protected function request($type, $query, array $previousRedirections = [])
    {
        $processed_query = $this->preprocessQuery($query);
        $response = $this->executeQuery($processed_query, $type);

        // Determine if the response is successful
        if ($response instanceof Http\Response) {
            $isSuccessful = $response->isSuccessful();
            $statusCode = $response->getStatus();
            $location = $response->getHeader('Location');
        } elseif ($response instanceof \GuzzleHttp\Psr7\Response) {
            $isSuccessful = $response->getStatusCode() >= 200 && $response->getStatusCode() < 300;
            $statusCode = $response->getStatusCode();
            $location = $response->getHeaderLine('Location');
        } else {
            throw new Http\Exception('Unsupported response type');
        }

        if (!$isSuccessful) {
            if ($location && !\in_array($location, $previousRedirections)) {
                switch ($type) {
                    case 'query':
                        $previousRedirections[] = $this->queryUri;
                        $this->queryUri = $location;
                        break;
                    case 'update':
                        $previousRedirections[] = $this->updateUri;
                        $this->updateUri = $location;
                        break;
                }
                $previousRedirections[] = $query;

                return $this->request($type, $query, $previousRedirections);
            } elseif ($location) {
                throw new Http\Exception('Circular redirection');
            }

            // Use the appropriate exception based on the response type
            throw new Http\Exception('HTTP request for SPARQL query failed', $statusCode, null, $response->getBody());
        }

        if (204 == $statusCode) {
            // No content
            return $response;
        }

        return $this->parseResponseToQuery($response);
    }


    protected function convertToTriples($data)
    {
        if (\is_string($data)) {
            return $data;
        } elseif (\is_object($data) && $data instanceof Graph) {
            // FIXME: insert Turtle when there is a way of seperateing out the prefixes
            return $data->serialise('ntriples');
        } else {
            throw new Exception("Don't know how to convert to triples for SPARQL query");
        }
    }

    /**
     * Adds missing prefix-definitions to the query
     *
     * Overriding classes may execute arbitrary query-alteration here
     *
     * @param string $query
     *
     * @return string
     */
    protected function preprocessQuery($query)
    {
        // Check for undefined prefixes
        $prefixes = '';
        foreach (RdfNamespace::namespaces() as $prefix => $uri) {
            if (str_contains($query, "{$prefix}:")
                  && !str_contains($query, "PREFIX {$prefix}:")
            ) {
                $prefixes .= "PREFIX {$prefix}: <{$uri}>\n";
            }
        }

        return $prefixes.$query;
    }

    /**
     * Build http-client object, execute request and return a response
     *
     * @param string $processed_query
     * @param string $type            Should be either "query" or "update"
     *
     * @return Http\Response|\Zend\Http\Response|\Laminas\Http\Client|\GuzzleHttp\ClientInterface
     *
     * @throws Exception
     */
    protected function executeQuery($processed_query, $type)
    {
        $client = Http::getDefaultHttpClient();

        // Reset parameters if the client is not an instance of GuzzleHTTP.
        if (!$client instanceof \GuzzleHttp\Client) {
            $client->resetParameters();
        }

        // Define the headers for SPARQL results and graph types
        $sparql_results_types = [
            'application/sparql-results+json' => 1.0,
            'application/sparql-results+xml' => 0.8,
        ];
        $sparql_graph_types = [
            'application/ld+json' => 1.0,
            'application/rdf+xml' => 0.9,
            'text/turtle' => 0.8,
            'application/n-quads' => 0.7,
            'application/n-triples' => 0.7,
        ];

        // Prepare headers and options
        $headers = [];

        if ('update' === $type) {
            // SPARQL update query
            $accept = Format::getHttpAcceptHeader($sparql_results_types);
            $headers['Accept'] = $accept;
            $headers['Content-Type'] = 'application/sparql-update';

            // Use Guzzle or Laminas to send the request
            $options = ['headers' => $headers, 'body' => $processed_query];

            return $this->sendRequest($client, $this->updateUri, 'POST', $options);

        } elseif ('query' === $type) {
            // Handle SPARQL query logic
            $query_verb = $this->determineQueryVerb($processed_query, $sparql_results_types, $sparql_graph_types);
            $accept = $this->determineAcceptHeader($query_verb, $sparql_results_types, $sparql_graph_types);
            $headers['Accept'] = $accept;

            // Encode the query for GET or POST
            $encodedQuery = 'query=' . urlencode($processed_query);
            $delimiter = $this->queryUri_has_params ? '&' : '?';

            // Decide whether to use GET or POST
            if (strlen($encodedQuery) + strlen($this->queryUri) <= 2046) {
                // Use GET request
                return $this->sendRequest($client, $this->queryUri . $delimiter . $encodedQuery, 'GET', ['headers' => $headers]);
            } else {
                // Use POST request
                $headers['Content-Type'] = 'application/x-www-form-urlencoded';
                return $this->sendRequest($client, $this->queryUri, 'POST', ['headers' => $headers, 'body' => $encodedQuery]);
            }
        } else {
            throw new Exception('unexpected request-type: ' . $type);
        }
    }

    protected function sendRequest($client, $uri, $method, $options)
    {
        if ($client instanceof \GuzzleHttp\ClientInterface) {
            try {
                return $client->request($method, $uri, $options);
            } catch (\GuzzleHttp\Exception\RequestException $e) {
                throw new Exception('Guzzle HTTP request failed: ' . $e->getMessage());
            }
        } elseif ($client instanceof \Laminas\Http\Client
            || $client instanceof \Zend\Http\Client
            || $client instanceof Http\Client) {
            $client->setMethod($method);
            $client->setUri($uri);
            if (isset($options['headers'])) {
                foreach ($options['headers'] as $name => $value) {
                    $client->setHeaders($name, $value);
                }
            }
            if (isset($options['body'])) {
                $client->setRawData($options['body']);
            }
            return $client->request(); // Execute the request
        } else {
            throw new Exception('Unsupported client type');
        }
    }

    protected function determineQueryVerb($processed_query, $sparql_results_types, $sparql_graph_types)
    {
        $re = '(?:(?:\s*BASE\s*<.*?>\s*)|(?:\s*PREFIX\s+.+:\s*<.*?>\s*))*' .
            '(CONSTRUCT|SELECT|ASK|DESCRIBE)[\W]';

        $result = null;
        $matched = mb_eregi($re, $processed_query, $result);

        if (!$matched || 2 !== count($result)) {
            // Non-standard query
            return null;
        } else {
            return strtoupper($result[1]);
        }
    }

    protected function determineAcceptHeader($query_verb, $sparql_results_types, $sparql_graph_types)
    {
        if ('SELECT' === $query_verb || 'ASK' === $query_verb) {
            return Format::formatAcceptHeader($sparql_results_types);
        } elseif ('CONSTRUCT' === $query_verb || 'DESCRIBE' === $query_verb) {
            return Format::formatAcceptHeader($sparql_graph_types);
        } else {
            return Format::getHttpAcceptHeader($sparql_results_types);
        }
    }

    /**
     * Parse HTTP-response object into a meaningful result-object.
     *
     * Can be overridden to do custom processing
     *
     * @param Http\Response|\Zend\Http\Response|\GuzzleHttp\Psr7\Response $response
     *
     * @return Graph|Result
     */
    protected function parseResponseToQuery($response)
    {
        // Check if the client is Guzzle or Laminas/Zend and retrieve Content-Type accordingly
        if ($response instanceof \GuzzleHttp\Psr7\Response) {
            $content_type = $response->getHeaderLine('Content-Type');
            $responseBody = (string)$response->getBody(); // Convert Guzzle response body to string
        } else {
            $content_type = $response->getHeader('Content-Type');
            $responseBody = $response->getBody();
        }

        list($content_type) = Utils::parseMimeType($content_type);

        // Check the content type to decide whether to return Result or Graph
        if (str_starts_with($content_type, 'application/sparql-results')) {
            return new Result($responseBody, $content_type);
        } else {
            return new Graph($this->queryUri, $responseBody, $content_type);
        }
    }

    /**
     * Proxy function to allow usage of our Client as well as Zend\Http v2, GuzzleHTTP and Laminas\Http.
     *
     * Zend\Http\Client only accepts an array as first parameter, but our Client wants a name-value pair.
     *
     * @see https://framework.zend.com/apidoc/2.4/classes/Zend.Http.Client.html#method_setHeaders
     *
     * @todo Its only a temporary fix, should be replaced or refined in the future.
     */
    protected function setHeaders($client, $name, $value)
    {
        if ($client instanceof Http\Client) {
            $client->setHeaders($name, $value);
        } else {
            $client->setHeaders([$name => $value]);
        }
    }
}
