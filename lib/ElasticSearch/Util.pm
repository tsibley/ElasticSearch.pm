package ElasticSearch::Util;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Error();

require Exporter;
our @ISA       = qw(Exporter);
our @EXPORT_OK = qw(parse_params throw build_error);

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

=head1 LICENSE AND COPYRIGHT

Copyright 2010 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;

