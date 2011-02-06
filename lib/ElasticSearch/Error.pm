package ElasticSearch::Error;

@ElasticSearch::Error::Internal::ISA    = __PACKAGE__;
@ElasticSearch::Error::Param::ISA       = __PACKAGE__;
@ElasticSearch::Error::NoServers::ISA   = __PACKAGE__;
@ElasticSearch::Error::Request::ISA     = __PACKAGE__;
@ElasticSearch::Error::Timeout::ISA     = __PACKAGE__;
@ElasticSearch::Error::Connection::ISA  = __PACKAGE__;
@ElasticSearch::Error::JSON::ISA        = __PACKAGE__;
@ElasticSearch::Error::QueryParser::ISA = __PACKAGE__;
@ElasticSearch::Error::Conflict::ISA
    = ( 'ElasticSearch::Error::Request', __PACKAGE__ );
@ElasticSearch::Error::Missing::ISA
    = ( 'ElasticSearch::Error::Request', __PACKAGE__ );

use strict;
use warnings FATAL => 'all', NONFATAL => 'redefine';

use overload ( '""' => 'stringify' );
use Data::Dumper;

#===================================
sub stringify {
#===================================
    my $error = shift;
    local $Data::Dumper::Terse  = 1;
    local $Data::Dumper::Indent = 1;

    my $msg
        = '[ERROR] ** '
        . ( ref($error) || 'ElasticSearch::Error' ) . ' at '
        . $error->{-file}
        . ' line '
        . $error->{-line} . " : \n"
        . ( $error->{-text} || 'Missing error message' ) . "\n"
        . (
        $error->{-vars}
        ? "\nWith vars:" . Dumper( $error->{-vars} ) . "\n"
        : ''
        ) . ( $error->{'-stacktrace'} || '' );
    return $msg;
}

=head1 NAME

ElasticSearch::Error - Exception objects for ElasticSearch

=head1 DESCRIPTION

ElasticSearch::Error is a base class for exceptions thrown by any ElasticSearch
code.

There are several exception subclasses, which indicate different types of error.
All of them inherit from L<ElasticSearch::Error>, and all include:

    $error->{-text}         # error message
    $error->{-file}         # file where error was thrown
    $error->{-line}         # line where error was thrown

They may also include:

    $error->{-vars}         # Any relevant variables related to the error
    $error->{-stacktrace}   # A stacktrace, if $ElasticSearch::DEBUG == 1

Error objects can be stringified, and include all of the above information
in the string output.

=head1 EXCEPTION CLASSES

=over

=item * ElasticSearch::Error::Param

An incorrect parameter was passed in

=item * ElasticSearch::Error::Timeout

The request timed out

=item * ElasticSearch::Error::Connection

There was an error connecting to the current server

=item * ElasticSearch::Error::Request

There was some other error performing the request

=item * ElasticSearch::Error::Conflict

There was a versioning conflict while performing an index/create/delete
operation.  C<ElasticSearch::Error::Conflict> inherits from
C<ElasticSearch::Error::Request>.

=item * ElasticSearch::Error::Missing

Tried to get/delete a document or index that doesn't exist.
C<ElasticSearch::Error::Missing> inherits from
C<ElasticSearch::Error::Request>.

=item * ElasticSearch::Error::NoServers

No servers are available

=item * ElasticSearch::Error::JSON

There was an error parsing a JSON doc

=item * ElasticSearch::Error::Internal

An internal error - you shouldn't see these

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;

