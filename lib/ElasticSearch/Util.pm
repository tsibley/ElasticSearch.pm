package ElasticSearch::Util;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Error();

require Exporter;
our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(filter_keywords parse_params throw build_error);

#===================================
sub filter_keywords {
#===================================
    local $_ = shift;

    s{[^[:alpha:][:digit:] \-+'"*@\._]+}{ }g;

    return '' unless /[[:alpha:][:digit:]]/;

    s/\s*\b(?:and|or|not)\b\s*/ /gi;

    # remove '-' that don't have spaces before them
    s/(?<! )-/\ /g;

    # remove the spaces after a + or -
    s/([+-])\s+/$1/g;

    # remove + or - not followed by a letter, number or "
    s/[+-](?![[:alpha:][:digit:]"])/ /g;

    # remove * without 3 char prefix
    s/(?<![[:alpha:][:digit:]\-@\._]{3})\*/ /g;

    my $quotes = (tr/"//);
    if ( $quotes % 2 ) { $_ .= '"' }

    s/^\s+//;
    s/\s+$//;

    return $_;
}

#===================================
sub parse_params {
#===================================
    my $self = shift;
    my $params;
    if ( @_ % 2 ) {
        $self->throw(
            "Param",
            'Expecting a HASH ref or a list of key-value pairs',
            { params => \@_ }
        ) unless ref $_[0] eq 'HASH';
        $params = shift;
    }
    else {
        $params = {@_};
    }
    return ( $self, $params );
}

#===================================
sub throw {
#===================================
    my ( $self, $type, $msg, $vars ) = @_;
    die build_error( $self, $type, $msg, $vars, 1 );
}

#===================================
sub build_error {
#===================================
    my $self   = shift;
    my $type   = shift;
    my $msg    = shift;
    my $vars   = shift;
    my $caller = shift || 0;

    my $class = ref $self || $self;
    my $error_class = 'ElasticSearch::Error::' . $type;

    $msg = 'Unknown error' unless defined $msg;
    $msg =~ s/\n/\n    /g;

    my ( undef, $file, $line ) = caller($caller);
    my $error_params = {
        -text => $msg,
        -line => $line,
        -file => $file,
        -vars => $vars,
    };
    {
        no warnings 'once';
        $error_params->{-stacktrace} = _stack_trace()
            if $ElasticSearch::DEBUG;
    }
    return bless $error_params, $error_class;

}

#===================================
sub _stack_trace {
#===================================
    my $i    = 2;
    my $line = ( '-' x 60 ) . "\n";
    my $o    = $line
        . sprintf( "%-4s %-30s %-5s %s\n",
        ( '#', 'Package', 'Line', 'Sub-routine' ) )
        . $line;
    while ( my @caller = caller($i) ) {
        $o .= sprintf( "%-4d %-30s %4d  %s\n", $i++, @caller[ 0, 2, 3 ] );
    }
    return $o .= $line;
}

=head1 NAME

ElasticSearch::Util - Util subs for ElasticSearch

=head1 DESCRIPTION

ElasticSearch::Util provides various subs useful to other modules in
ElasticSearch.

The only sub useful to users is L</"filter_keywords()">, which can be
exported.

=head1 SYNOPSIS

    use ElasticSearch::Util qw(filter_keywords);

    my $filtered = filter_keywords($unfiltered)

=head1 SUBROUTINES

=head2 C<filter_keywords()>

This tidies up a string to be used as a query string in (eg)
L<ElasticSearch/"search()"> so that user input won't cause a search query
to return an error.

It is not flexible at all, and may or may not be useful to you.

Have a look at L<ElasticSearch::QueryParser> which gives you much more control
over your query strings.

The current implementation does the following:

=over

=item * Removes any character which isn't a letter, a number, a space or
  C<-+'"*@._>.

=item * Removes C<and>, C<or> and C<not>

=item * Removes any C<-> that doesn't have a space in front of it ( "foo -bar")
      is acceptable as it means C<'foo' but not with 'bar'>

=item * Removes any space after a C<+> or C<->

=item * Removes any C<+> or C<-> which is not followed by a letter, number
      or a double quote

=item * Removes any C<*> that doesn't have at least 3 letters before it, ie
      we only allow wildcard searches on words with at least 3 characters

=item * Closes any open double quotes

=item * Removes leading and trailing whitespace

=back

YMMV

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;

