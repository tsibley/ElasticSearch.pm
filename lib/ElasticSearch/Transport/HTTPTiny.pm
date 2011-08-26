package ElasticSearch::Transport::HTTPTiny;

use strict;
use warnings FATAL => 'all';
use HTTP::Tiny();
use Encode qw(decode_utf8);

use parent 'ElasticSearch::Transport';

my $Connection_Error = qr/ Connection.(?:timed.out|re(?:set|fused))
                       | No.route.to.host
                       | temporarily.unavailable
                       /x;

#===================================
sub protocol     {'http'}
sub default_port {9200}
#===================================

#===================================
sub send_request {
#===================================
    my $self   = shift;
    my $server = shift;
    my $params = shift;

    my $method = $params->{method};
    my $uri    = $self->http_uri( $server, $params->{cmd}, $params->{qs} );
    my $client = $self->client;

    my $opts = {};
    if ( my $data = $params->{data} ) {
        utf8::encode($data);
        $opts = {
            content => $data,
            headers =>
                { 'content-type' => 'application/x-www-form-urlencoded' }
        };
    }

    my $response = $client->request( $method, $uri, $opts );

    my $code    = $response->{status};
    my $msg     = $response->{reason};
    my $content = decode_utf8( $response->{content} || '' );

    return $content if $code && $code >= 200 && $code <= 209;

    if ( $code eq '599' ) {
        $msg     = $content;
        $content = '';
    }

    my $type
        = $code eq '409' ? 'Conflict'
        : $code eq '404' ? 'Missing'
        : $msg =~ /Timed out/         ? 'Timeout'
        : $msg =~ /$Connection_Error/ ? 'Connection'
        :                               'Request';

    my $error_params = {
        server      => $server,
        status_code => $code,
        status_msg  => $msg,
    };

    if ( $type eq 'Request' or $type eq 'Conflict' or $type eq 'Missing' ) {
        $error_params->{content} = $content;
    }
    $self->throw( $type, $msg . ' (' . ( $code || 500 ) . ')',
        $error_params );
}

#===================================
sub client {
#===================================
    my $self = shift;
    unless ( $self->{_client}{$$} ) {
        my $client = HTTP::Tiny->new( timeout => $self->timeout || 10000 );
        $self->{_client}{$$} = $client;
    }
    return $self->{_client}{$$};
}

=head1 NAME

ElasticSearch::Transport::HTTPTiny - HTTP::Tiny based HTTP backend

=head1 DESCRIPTION

ElasticSearch::Transport::HTTPTiny uses L<HTTP::Tiny> to talk to ElasticSearch
over HTTP.

It is slightly (1%) faster thab L<ElasticSearch::Transport::HTTPLite>.


=head1 SYNOPSIS

    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'httptiny',
        timeout     => '10',
    );

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport>

=item * L<ElasticSearch::Transport::HTTP>

=item * L<ElasticSearch::Transport::HTTPLite>

=item * L<ElasticSearch::Transport::Curl>

=item * L<ElasticSearch::Transport::AEHTTP>

=item * L<ElasticSearch::Transport::Thrift>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 - 2011 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
