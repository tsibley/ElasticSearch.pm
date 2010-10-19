package ElasticSearch;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Transport();
use ElasticSearch::Error();
use ElasticSearch::RequestParser;
use ElasticSearch::Util qw(throw parse_params);

our $VERSION = '0.22';
our $DEBUG   = 0;

#===================================
sub new {
#===================================
    my ( $proto, $params ) = parse_params(@_);
    my $self = { _base_qs => {}, };

    bless $self, ref $proto || $proto;
    $self->{_transport} = ElasticSearch::Transport->new($params);
    $self->$_( $params->{$_} ) for keys %$params;
    return $self;
}

#===================================
sub request {
#===================================
    my ( $self, $params ) = parse_params(@_);
    return $self->transport->request($params);
}

#===================================
sub transport   { shift()->{_transport} }
sub trace_calls { shift->transport->trace_calls(@_) }
#===================================

=head1 NAME

ElasticSearch - An API for communicating with ElasticSearch

=head1 VERSION

Version 0.22, tested against ElasticSearch server version 0.12.0.

NOTE: This version has been completely refactored, to provide multiple
Transport backends, and some methods have moved to subclasses.

=head1 DESCRIPTION

ElasticSearch is an Open Source (Apache 2 license), distributed, RESTful
Search Engine based on Lucene, and built for the cloud, with a JSON API.

Check out its features: L<http://www.elasticsearch.com/products/elasticsearch/>

This module is a thin API which makes it easy to communicate with an
ElasticSearch cluster.

It maintains a list of all servers/nodes in the ElasticSearch cluster, and
spreads the load randomly across these nodes.  If the current active node
disappears, then it attempts to connect to another node in the list.

Forking a process triggers a server list refresh, and a new connection to
a randomly chosen node in the list.

=cut

=head1 SYNOPSIS


    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'http' | 'httplite' | 'thrift', # default 'http'
        trace_calls => 'log_file',
    );

    $e->index(
        index => 'twitter',
        type  => 'tweet',
        id    => 1,
        data  => {
            user        => 'kimchy',
            post_date   => '2009-11-15T14:12:12',
            message     => 'trying out Elastic Search'
        }
    );

    $data = $e->get(
        index => 'twitter',
        type  => 'tweet',
        id    => 1
    );

    $results = $e->search(
        index => 'twitter',
        type  => 'tweet',
        query => {
            term    => { user => 'kimchy' },
        }
    );

    $results = $e->search(
        index => 'twitter',
        type  => 'tweet',
        query => {
            query_string => { query => 'kimchy' },
        }
    );

=cut

=head1 GETTING ElasticSearch

You can download the latest released version of ElasticSearch from
L<http://github.com/elasticsearch/elasticsearch/downloads>.

See here for setup instructions:
L<http://github.com/elasticsearch/elasticsearch/wiki/Setting-up-ElasticSearch>

=cut

=head1 CALLING CONVENTIONS

I've tried to follow the same terminology as used in the ElasticSearch docs
when naming methods, so it should be easy to tie the two together.

Some methods require a specific C<index> and a specific C<type>, while others
allow a list of indices or types, or allow you to specify all indices or
types. I distinguish between them as follows:

   $e->method( index => multi, type => single, ...)

C<multi> values can be:

      index   => 'twitter'          # specific index
      index   => ['twitter','user'] # list of indices
      index   => undef              # (or not specified) = all indices

C<single> values must be a scalar, and are required parameters

      type  => 'tweet'

=cut

=head1 RETURN VALUES AND EXCEPTIONS

Methods that query the ElasticSearch cluster return the raw data structure
that the cluster returns.  This may change in the future, but as these
data structures are still in flux, I thought it safer not to try to interpret.

Anything that is known to be an error throws an exception, eg trying to delete
a non-existent index.

=cut

=head1 METHODS

=head2 Creating a new ElasticSearch instance

=head3 C<new()>

    $e = ElasticSearch->new(
            servers     =>  '127.0.0.1:9200'            # single server
                            | ['es1.foo.com:9200',
                               'es2.foo.com:9200'],     # multiple servers
            trace_calls => 1 | '/path/to/log/file',
            debug       => 1 | 0,
            timeout     => 30,

     );

C<servers> is a required parameter and can be either a single server or an
ARRAY ref with a list of servers.  These servers are used to retrieve a list
of all servers in the cluster, after which one is chosen at random to be
the L</"current_server()">.

See also: L</"debug()">, L</"timeout()">, L</"trace_calls()">,
          L</"refresh_servers()">, L</"servers()">, L</"current_server()">

=cut

=head2 Document-indexing methods

=head3 C<index()>

    $result = $e->index(
        index   => single,
        type    => single,
        id      => $document_id,        # optional, otherwise auto-generated
        data    => {
            key => value,
            ...
        },
        timeout => eg '1m' or '10s'     # optional
        create  => 1 | 0                # optional
        refresh => 1 | 0                # optional
    );

eg:

    $result = $e->index(
        index   => 'twitter',
        type    => 'tweet',
        id      => 1,
        data    => {
            user        => 'kimchy',
            post_date   => '2009-11-15T14:12:12',
            message     => 'trying out Elastic Search'
        },
    );

Used to add a document to a specific C<index> as a specific C<type> with
a specific C<id>. If the C<index/type/id> combination already exists,
then that document is updated, otherwise it is created.

Note:

=over

=item *

If the C<id> is not specified, then ElasticSearch autogenerates a unique
ID and a new document is always created.

=item *

If C<create> is C<true>, then a new document is created, even if the same
C<index/type/id> combination already exists!  C<create> can be used to
slightly increase performance when creating documents that are known not
to exists in the index.

=back

See also: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/index>,
L</"bulk()"> and L</"put_mapping()">


=head3 C<set()>

C<set()> is a synonym for L</"index()">


=head3 C<create()>

C<create> is a synonym for L</"index()"> but creates instead of first checking
whether the doc already exists. This speeds up the indexing process.

=head3 C<get()>

    $result = $e->get(
        index   => single,
        type    => single,
        id      => single,
    );

Returns the document stored at C<index/type/id> or throws an exception if
the document doesn't exist.

Example:

    $e->get( index => 'twitter', type => 'tweet', id => 1)
    Returns:
    {
      _id     => 1,
      _index  => "twitter",
      _source => {
                   message => "trying out Elastic Search",
                   post_date=> "2009-11-15T14:12:12",
                   user => "kimchy",
                 },
      _type   => "tweet",
    }

See also: L</"bulk()">, L<KNOWN ISSUES>,
          L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/get>

=head3 C<delete()>

    $result = $e->delete(
        index   => single,
        type    => single,
        id      => single,
        refresh => 1 | 0                # optional
    );

Deletes the document stored at C<index/type/id> or throws an exception if
the document doesn't exist.

Example:

    $e->delete( index => 'twitter', type => 'tweet', id => 1);

See also: L</"bulk()">,
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/delete>

=head3 C<bulk()>

    $result = $e->bulk([
        { create => { index => 'foo', type => 'bar', id => 123,
                      data => { text => 'foo bar'}              }},
        { index  => { index => 'foo', type => 'bar', id => 123,
                      data => { text => 'foo bar'}              }},
        { delete => { index => 'foo', type => 'bar', id => 123  }},
    ]);

Perform multiple C<index>,C<create> or C<delete> operations in a single
request.  In my benchmarks, this is 10 times faster than serial operations.

For the above example, the C<$result> will look like:

    {
        actions => [ the list of actions you passed in ],
        results => [
                 { create => { id => 123, index => "foo", type => "bar" } },
                 { index  => { id => 123, index => "foo", type => "bar" } },
                 { delete => { id => 123, index => "foo", type => "bar" } },
        ]
    }

where each row in C<results> corresponds to the same row in C<actions>.
If there are any errors for individual rows, then the C<$result> will contain
a key C<errors> which contains an array of each error and the associated
action, eg:

    $result = {
        actions => [

            ## NOTE - num is numeric
            {   index => { index => 'bar', type  => 'bar', id => 123,
                           data  => { num => 123 } } },

            ## NOTE - num is a string
            {   index => { index => 'bar', type  => 'bar', id => 123,
                           data  => { num => 'foo bar' } } },
        ],
        errors => [
            {
                action => {
                    index => { index => 'bar', type  => 'bar', id => 123,
                               data  => { num => 'text foo' } }
                },
                error => "MapperParsingException[Failed to parse [num]]; ...",
            },
        ],
        results => [
            { index => { id => 123, index => "bar", type => "bar" } },
            {   index => {
                    error => "MapperParsingException[Failed to parse [num]];...",
                    id    => 123, index => "bar", type  => "bar",
                },
            },
        ],

    };

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/bulk> for
more details.

=cut

=head2 Query commands

=head3 C<search()>

    $result = $e->search(
        index           => multi,
        type            => multi,
        query           => {query},
        search_type     => $search_type             # optional
        explain         => 1 | 0                    # optional
        facets          => { facets }               # optional
        fields          => [$field_1,$field_n]      # optional
        from            => $start_from              # optional
        script_fields   => { script_fields }        # optional
        size            => $no_of_results           # optional
        sort            => ['_score',$field_1]      # optional
        scroll          => '5m' | '30s'             # optional
        highlight       => { highlight }            # optional
        indices_boost   => { index_1 => 1.5,... }   # optional
    );

Searches for all documents matching the query. Documents can be matched
against multiple indices and multiple types, eg:

    $result = $e->search(
        index   => undef,                           # all
        type    => ['user','tweet'],
        query   => { term => {user => 'kimchy' }}
    );

For all of the options that can be included in the C<query> parameter, see
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/search> and
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>

=head3 C<scroll()>

    $result = $e->scroll(scroll_id => $scroll_id );

If a search has been executed with a C<scroll> parameter, then the returned
C<scroll_id> can be used like a cursor to scroll through the rest of the
results.

Note - this doesn't seem to work correctly in version 0.12.0 of ElasticSearch.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/search/#Scrolling>

=head3 C<count()>

    $result = $e->count(
        index           => multi,
        type            => multi,

        bool
      | constant_score
      | custom_score
      | dis_max
      | field
      | filtered
      | flt
      | flt_field
      | fuzzy
      | match_all
      | mlt
      | mlt_field
      | query_string
      | prefix
      | range
      | span_term
      | span_first
      | span_near
      | span_not
      | span_or
      | term
      | wildcard
    );

Counts the number of documents matching the query. Documents can be matched
against multiple indices and multiple types, eg

    $result = $e->count(
        index   => undef,               # all
        type    => ['user','tweet'],
        term => {user => 'kimchy' },
    );

See also L</"search()">,
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/count>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>


=head3 C<delete_by_query()>

    $result = $e->delete_by_query(
        index           => multi,
        type            => multi,

        bool
      | constant_score
      | custom_score
      | dis_max
      | field
      | filtered
      | flt
      | flt_field
      | fuzzy
      | match_all
      | mlt
      | mlt_field
      | query_string
      | prefix
      | range
      | span_term
      | span_first
      | span_near
      | span_not
      | span_or
      | term
      | wildcard
    );

Deletes any documents matching the query. Documents can be matched against
multiple indices and multiple types, eg

    $result = $e->delete_by_query(
        index   => undef,               # all
        type    => ['user','tweet'],
        term    => {user => 'kimchy' }
    );

See also L</"search()">,
L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/delete_by_query>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/>


=head3 C<mlt()>

    # mlt == more_like_this

    $results = $e->mlt(
        index               => single,              # required
        type                => single,              # required
        id                  => $id,                 # required

        # optional more-like-this params
        boost_terms          =>  float
        mlt_fields           =>  'scalar' or ['scalar_1', 'scalar_n']
        max_doc_freq         =>  integer
        max_query_terms      =>  integer
        max_word_len         =>  integer
        min_doc_freq         =>  integer
        min_term_freq        =>  integer
        min_word_len         =>  integer
        pct_terms_to_match   =>  float
        stop_words           =>  'scalar' or ['scalar_1', 'scalar_n']

        # optional search params
        scroll               =>  '5m' | '10s'
        search_type          =>  "predefined_value"
        explain              =>  {explain}
        facets               =>  {facets}
        fields               =>  {fields}
        from                 =>  {from}
        highlight            =>  {highlight}
        size                 =>  {size}
        sort                 =>  {sort}

    )

More-like-this (mlt) finds related/similar documents. It is possible to run
a search query with a C<more_like_this> clause (where you pass in the text
you're trying to match), or to use this method, which uses the text of
the document referred to by C<index/type/id>.

This gets transformed into a search query, so all of the search parameters
are also available.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/more_like_this/>
and L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/more_like_this_query/>

=cut

=head2 Index Admin methods

=head3 C<index_status()>

    $result = $e->index_status(
        index   => multi,
    );

Returns the status of
    $result = $e->index_status();                               #all
    $result = $e->index_status( index => ['twitter','buzz'] );
    $result = $e->index_status( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/status>

=head3 C<create_index()>

    $result = $e->create_index(
        index   => single,
        defn    => {...}        # optional
    );

Creates a new index, optionally setting certain paramters, eg:

    $result = $e->create_index(
        index   => 'twitter',
        defn    => {
                number_of_shards      => 3,
                number_of_replicas    => 2,
        }
    );

Throws an exception if the index already exists.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/create_index>

=head3 C<delete_index()>

    $result = $e->delete_index(
        index   => single
    );

Deletes an existing index, or throws an exception if the index doesn't exist, eg:

    $result = $e->delete_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_index>

=head3 C<update_index_settings()>

    $result = $e->update_index_settings(
        index       => multi,
        settings    => { ... settings ...}
    );

Update the settings for all, one or many indices.  Currently only the
C<number_of_replicas> is exposed:

    $result = $e->update_index_settings(
        settings    => {  number_of_replicas => 1 }
    );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/update_settings/>

=head3 C<aliases()>

    $result = $e->aliases( actions => [actions] | {actions} )

Adds or removes an alias for an index, eg:

    $result = $e->aliases( actions => [
                { remove => { index => 'foo', alias => 'bar' }},
                { add    => { index => 'foo', alias => 'baz'  }}
              ]);

C<actions> can be a single HASH ref, or an ARRAY ref containing multiple HASH
refs.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/aliases/>

=head3 C<get_aliases()>

    $result = $e->get_aliases( index => multi )

Returns a hashref listing all indices and their corresponding aliases, and
all aliases and their corresponding indices, eg:

    {
      aliases => {
         bar => ["foo"],
         baz => ["foo"],
      },
      indices => { foo => ["baz", "bar"] },
    }

If you pass in the optional C<index> argument, which can be an index name
or an alias name, then it will only return the indices and aliases related
to that argument.

=head3 C<flush_index()>

    $result = $e->flush_index(
        index   => multi,
        full    => 1 | 0,       # optional
        refresh => 1 | 0,       # optional
    );

Flushes one or more indices, which frees
memory from the index by flushing data to the index storage and clearing the
internal transaction log. By default, ElasticSearch uses memory heuristics
in order to automatically trigger flush operations as required in order to
clear memory.

Example:

    $result = $e->flush_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/flush>

=head3 C<refresh_index()>

    $result = $e->refresh_index(
        index   => multi
    );

Explicitly refreshes one or more indices, making all operations performed
since the last refresh available for search. The (near) real-time capabilities
depends on the index engine used. For example, the robin one requires
refresh to be called, but by default a refresh is scheduled periodically.

Example:

    $result = $e->refresh_index( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh>

=head3 C<clear_cache()>

    $result = $e->clear_cache( index => multi );

Clears the caches for the specified indices (currently only the filter cache).

See L<http://github.com/elasticsearch/elasticsearch/issues/issue/101>

=head3 C<gateway_snapshot()>

    $result = $e->gateway_snapshot(
        index   => multi
    );

Explicitly performs a snapshot through the gateway of one or more indices
(backs them up ). By default, each index gateway periodically snapshot changes,
though it can be disabled and be controlled completely through this API.

Example:

    $result = $e->gateway_snapshot( index => 'twitter' );

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/gateway_snapshot>
and L<http://www.elasticsearch.com/docs/elasticsearch/modules/gateway>

=head3 C<snapshot_index()>

C<snapshot_index()> is a synonym for L</"gateway_snapshot()">

=head3 C<optimize_index()>

    $result = $e->optimize_index(
        index           => multi,
        only_deletes    => 1 | 0,  # only_expunge_deletes
        flush           => 1 | 0,  # flush after optmization
        refresh         => 1 | 0,  # refresh after optmization
    )

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/optimize>

=head3 C<put_mapping()>

    $result = $e->put_mapping(
        index               => multi,
        type                => single,
        _all                => { ... },
        _source             => { ... },
        properties          => { ... },      # required
        timeout             => '5m' | '10s', # optional
        ignore_conflicts    => 1 | 0,        # optional
    );

A C<mapping> is the data definition of a C<type>.  If no mapping has been
specified, then ElasticSearch tries to infer the types of each field in
document, by looking at its contents, eg

    'foo'       => string
    123         => integer
    1.23        => float

However, these heuristics can be confused, so it safer (and much more powerful)
to specify an official C<mapping> instead, eg:

    $result = $e->put_mapping(
        index   => ['twitter','buzz'],
        type    => 'tweet',
        _source => { compress => 1 },
        properties  =>  {
            user        =>  {type  =>  "string", index      =>  "not_analyzed"},
            message     =>  {type  =>  "string", null_value =>  "na"},
            post_date   =>  {type  =>  "date"},
            priority    =>  {type  =>  "integer"},
            rank        =>  {type  =>  "float"}
        }
    );

See also: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/put_mapping>
and L<http://www.elasticsearch.com/docs/elasticsearch/mapping>

=head3 C<delete_mapping()>

    $result = $e->delete_mapping(
        index   => multi,
        type    => single,
    );

Deletes a mapping/type in one or more indices.
See also L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_mapping>

=head3 C<mapping()>

    $mapping = $e->mapping(
        index       => single,
        type        => multi
    );

Returns the mappings for all types in an index, or the mapping for the specified
type(s), eg:

    $mapping = $e->mapping(
        index       => 'twitter',
        type        => 'tweet'
    );

    $mappings = $e->mapping(
        index       => 'twitter',
        type        => ['tweet','user']
    );
    # { twitter => { tweet => {mapping}, user => {mapping}} }

Note: the index name which as used in the results is the actual index name. If
you pass an alias name as the C<index> name, then this key will be the
index (or indices) that the alias points to.


=cut

=head2 River admin methods

=head3 C<create_river()>

    $result = $e->create_river(
        river   => $river_name,     # required
        type    => $type,           # required
        $type   => {...},           # depends on river type
        index   => {...},           # depends on river type
    );

Creates a new river with name C<$name>, eg:

    $result = $e->create_river(
        river   => 'my_twitter_river',
        type    => 'twitter',
        twitter => {
            user        => 'user',
            password    => 'password',
        },
        index   => {
            index       => 'my_twitter_index',
            type        => 'status',
            bulk_size   => 100
        }
    )

See L<http://www.elasticsearch.com/docs/elasticsearch/river/>
and L<http://www.elasticsearch.com/docs/elasticsearch/river/twitter/>.


=head3 C<get_river()>

    $result = $e->get_river( river => $river_name );

Returns the river details eg

    $result = $e->get_river ( river => 'my_twitter_river' )

Throws an exception if the river doesn't exist.

See L<http://www.elasticsearch.com/docs/elasticsearch/river/>.


=head3 C<delete_river()>

    $result = $e->delete_river( river => $river_name );

Deletes the corresponding river, eg:

    $result = $e->delete_river ( river => 'my_twitter_river' )

Throws an exception if the river doesn't exist.

=head2 Cluster admin methods

=head3 C<cluster_state()>

    $result = $e->cluster_state(
         filter_nodes           => 1 | 0,                        # optional
         filter_metadata        => 1 | 0,                        # optional
         filter_routing_table   => 1 | 0,                        # optional
         filter_indices         => [ 'index_1', ... 'index_n' ], # optional
    );

Returns cluster state information.

See L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/state/>

=head3 C<cluster_health()>

    $result = $e->cluster_health(
        index                         => multi,
        level                         => 'cluster' | 'indices' | 'shards',
        wait_for_status               => 'red' | 'yellow' | 'green',
        | wait_for_relocating_shards  => $number_of_shards,
        | wait_for_nodes              => eg '>=2',
        timeout                       => $seconds
    );

Returns the status of the cluster, or index|indices or shards, where the
returned status means:

=over

=item red: Data not allocated

=item yellow: Primary shard allocated

=item green: All shards allocated

=back

It can block to wait for a particular status (or better), or can block to
wait until the specified number of shards have been relocated (where 0 means
all) or the specified number of nodes have been allocated.

If waiting, then a timeout can be specified.

For example:

    $result = $e->cluster_health( wait_for_status => 'green', timeout => '10s')

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/health/>

=head3 C<nodes()>

    $result = $e->nodes(
        nodes       => multi,
        settings    => 1 | 0        # optional
    );

Returns information about one or more nodes or servers in the cluster. If
C<settings> is C<true>, then it includes the node settings information.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_info>

=head3 C<nodes_stats()>

    $result = $e->nodes_stats(
        nodes       => multi,
    );

Returns various statistics about one or more nodes in the cluster.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_stats/>

=head3 C<shutdown()>

    $result = $e->shutdown(
        nodes       => multi,
        delay       => '5s' | '10m'        # optional
    );


Shuts down one or more nodes (or the whole cluster if no nodes specified),
optionally with a delay.

C<node> can also have the values C<_local>, C<_master> or C<_all>.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_shutdown/>

=head3 C<restart()>

    $result = $e->restart(
        nodes       => multi,
        delay       => '5s' | '10m'        # optional
    );


Restarts one or more nodes (or the whole cluster if no nodes specified),
optionally with a delay.

C<node> can also have the values C<_local>, C<_master> or C<_all>.

See: L<http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/cluster/nodes_restart>

=head3 C<current_server_version()>

    $version = $e->current_server_version()

Returns a HASH containing the version C<number> string, the build C<date> and
whether or not the current server is a C<snapshot_build>.

=cut


=head2 Other methods

=head3 C<trace_calls()>

    $es->trace_calls(1);            # log to STDERR
    $es->trace_calls($filename);    # log to $filename.$PID
    $es->trace_calls(0 | undef);    # disable logging

C<trace_calls()> is used for debugging.  All requests to the cluster
are logged either to C<STDERR> or the specified filename, with the
current $PID appended, in a form that can be rerun with curl.

The cluster response will also be logged, and commented out.

Example: C<< $e->cluster_health >> is logged as:

    # [Tue Oct 19 15:32:31 2010] Protocol: http, Server: 127.0.0.1:9200
    curl -XGET 'http://127.0.0.1:9200/_cluster/health'

    # [Tue Oct 19 15:32:31 2010] Response:
    # {
    #    "relocating_shards" : 0,
    #    "active_shards" : 0,
    #    "status" : "green",
    #    "cluster_name" : "elasticsearch",
    #    "active_primary_shards" : 0,
    #    "timed_out" : false,
    #    "initializing_shards" : 0,
    #    "number_of_nodes" : 1,
    #    "unassigned_shards" : 0
    # }

=head3 C<transport()>

    $transport = $e->transport

Returns the Transport object, eg L<ElasticSearch::Transport::HTTP>.

=head3 C<camel_case()>

    $bool = $e->camel_case($bool)

Gets/sets the camel_case flag. If true, then all JSON keys returned by
ElasticSearch are in camelCase, instead of with_underscores.  This flag
does not apply to the source document being indexed or fetched.

Defaults to false.

=cut

=head3 C<error_trace()>

    $bool = $e->error_trace($bool)

If the ElasticSearch server is returning an error, setting C<error_trace>
to true will return some internal information about where the error originates.
Mostly useful for debugging.

=cut

=head2 GLOBAL VARIABLES

    $Elasticsearch::DEBUG = 0 | 1;

If C<$Elasticsearch::DEBUG> is set to true, then ElasticSearch exceptions
will include a stack trace.

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 KNOWN ISSUES

=over

=item   L</"set()">, L</"index()"> and L</"create()">

If one of the fields that you are trying to index has the same name as the
type, then you need change the format as follows:

Instead of:

     $e->set(index=>'twitter', type=>'tweet',
             data=> { tweet => 'My tweet', date => '2010-01-01' }
     );

you should include the type name in the data:

     $e->set(index=>'twitter', type=>'tweet',
             data=> { tweet=> { tweet => 'My tweet', date => '2010-01-01' }}
     );

=item   L</"get()">

The C<_source> key that is returned from a L</"get()"> contains the original JSON
string that was used to index the document initially.  ElasticSearch parses
JSON more leniently than L<JSON::XS>, so if invalid JSON is used to index the
document (eg unquoted keys) then C<< $e->get(....) >> will fail with a
JSON exception.

Any documents indexed via this module will be not susceptible to this problem.

=item L</"scroll()">

C<scroll()> is broken in version 0.12.0 and earlier versions of ElasticSearch.

See L<http://github.com/elasticsearch/elasticsearch/issues/issue/136>

=back

=head1 BUGS

This is a beta module, so there will be bugs, and the API is likely to
change in the future, as the API of ElasticSearch itself changes.

If you have any suggestions for improvements, or find any bugs, please report
them to L<http://github.com/clintongormley/ElasticSearch.pm/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 TODO

Hopefully I'll be adding an ElasticSearch::Abstract (similar to
L<SQL::Abstract>) which will make it easier to generate valid queries
for ElasticSearch.

Also, a non-blocking L<AnyEvent> module has been written, but needs
integrating with the new L<ElasticSearch::Transport>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ElasticSearch


You can also look for information at:

=over 4

=item * GitHub

L<http://github.com/clintongormley/ElasticSearch.pm>

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=ElasticSearch>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/ElasticSearch>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/ElasticSearch>

=item * Search CPAN

L<http://search.cpan.org/dist/ElasticSearch/>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to Shay Bannon, the ElasticSearch author, for producing an amazingly
easy to use search engine.

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
