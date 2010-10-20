#!/user/bin/env perl

use strict;
use warnings;

use JSON;
use ElasticSearch();
use ElasticSearch::Util qw(filter_keywords);
use HTTP::Lite();

my $github_api = 'http://github.com/api/v2/json';
my $issues_url = '/issues/list/elasticsearch/elasticsearch/open';
my $Index      = 'issues';
my $Type       = 'entry';
my $ES         = ElasticSearch->new( servers => '127.0.0.1:9200' );

prepare_index();
my $issues = download_issues( $github_api . $issues_url );
index_issues($issues);

print <<USAGE;

    You can now search the database of open issues.
    Enter some keywords, eg:
        > kimchy
        > clint*

    Or enter an issue ID to see all of the details:
        > 1

USAGE

while (1) {
    my $keywords = get_keywords() or last;
    if ( $keywords =~ /^\d+$/ ) {
        retrieve_issue($keywords);
    }
    else {
        search_issues($keywords);
    }
}

#===================================
sub download_issues {
#===================================
    my $url  = shift;
    my $http = HTTP::Lite->new();
    my $req  = $http->request($url);
    die "couldn't retrieve issues list" unless $req && $req == 200;
    return decode_json( $http->body )->{issues};
}

#===================================
sub prepare_index {
#===================================

    # delete index in case it already exists, then create the index
    eval {
        $ES->delete_index( index => $Index );
        print " Deleted existing index '$Index'\n";
    };

    print " Creating index '$Index'\n";
    $ES->create_index( index => $Index );

}

#===================================
sub index_issues {
#===================================
    my $issues = shift;

    my $id = 1;
    my @docs;
    print "Preparing issues for indexing\n";
    for (@$issues) {

        # each doc needs an index, a type, an ID and data
        my $doc = { index => $Index, type => $Type, id => $id++, data => $_ };

        # we want to 'create' each doc (as opposed to 'index' or 'delete')
        push @docs, { create => $doc };
    }

    # bulk index docs
    print "Indexing issues\n";
    my $res = $ES->bulk( \@docs );
    if ( $res->{errors} ) {
        die "Bulk index had issues: " . encode_json( $res->{errors} );
    }

    # force all changes to be refreshed immediately
    $ES->refresh_index();

    my $total = $ES->count( index => $Index, match_all => {} )->{count};
    printf( "Total issues indexed: %d\n", $total );
}

#===================================
sub get_keywords {
#===================================
    print "\n" . "Enter keywords to search for, or an issue ID:" . "\n  > ";
    my $keywords = <>;
    chomp $keywords;
    last unless $keywords;

    # filter keywords to make sure the keywords don't include special chars
    return filter_keywords($keywords);
}

#===================================
sub search_issues {
#===================================
    my $keywords = shift;
    my $result   = $ES->search(
        index => $Index,
        query => {
            field =>
                { _all => $keywords }  # query string search across all fields
        }
    );
    printf( "Total results found for \"%s\": %d\n",
        $keywords, $result->{hits}{total} );
    printf( " - % 2d: %s\n", $_->{_id}, $_->{_source}{title} )
        for @{ $result->{hits}{hits} };
}

#===================================
sub retrieve_issue {
#===================================
    my $id  = shift;
    my $doc = eval {
        $ES->get(
            index => $Index,
            type  => $Type,
            id    => $id
        )->{_source};
        }
        or printf( "ERROR: Unknown doc ID: $id. We have docs 1..%d\n",
        $ES->count( index => $Index, match_all => {} )->{count} );

    for my $key ( sort keys %$doc ) {
        my $val = $doc->{$key} // '';
        print "$key: $val\n";
    }
    print( '-' x 60, "\n" );
}

=head1 DESCRIPTION

This demo script downloads all of the open ElasticSearch issues, and indexes
them into an index called 'issues'.

Then it presents you with a simple command line interface for searching
for issues with keywords, or if you type in the issue ID instead, it displays
all of the issue details.

In order to run this demo, you will need an ElasticSearch server running
on localhost. If you don't already have ElasticSearch, you can find the
latest version (currently 0.12.0) at L<http://www.elasticsearch.com/download/>.

You can install it as follows:

    wget http://cloud.github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.12.0.zip
    unzip elasticsearch-0.12.0.zip
    cd elasticsearch-0.12.0
    ./bin/elasticsearch -f                   # run server in foreground

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
