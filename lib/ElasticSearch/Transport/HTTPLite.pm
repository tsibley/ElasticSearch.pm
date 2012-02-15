package ElasticSearch::Transport::HTTPLite;

use strict;
use warnings FATAL => 'all';
use HTTP::Lite();
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
    $client->add_req_header( 'Accept-Encoding' => 'deflate' )
        if $self->deflate;

    $client->method($method);
    if ( my $data = $params->{data} ) {
        utf8::encode($data);
        $client->{content} = $data;
    }

    my $code = $client->request($uri) || 500;
    my $msg = $!;

    my $content = $client->body || '';

    my $ce = ( $client->get_header('Content-Encoding') || [] )->[0] || '';
    $content = $self->inflate($content) if $ce eq 'deflate';
    $content = decode_utf8 $content;

    return $content if $code && $code >= 200 && $code <= 209;

    $msg ||= $client->status_message || 'read timeout';
    my $type
        = $code eq '409' ? 'Conflict'
        : $code eq '404' ? 'Missing'
        : $code eq '403' ? 'ClusterBlocked'
        : $msg =~ /$Connection_Error/ ? 'Connection'
        : $msg =~ /read timeout/      ? 'Timeout'
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
        my $client = HTTP::Lite->new;
        $client->{timeout} = $self->timeout || 10000;
        $self->{_client}{$$} = $client;
    }
    my $client = $self->{_client}{$$};
    $client->reset;
    return $client;
}

=head1 NAME

ElasticSearch::Transport::HTTPLite - HTTP::Lite based HTTP backend

=head1 DESCRIPTION

ElasticSearch::Transport::HTTPLite uses L<HTTP::Lite> to talk to ElasticSearch
over HTTP.

It is a new backend and will probably become the default, as it is about 30%
faster than L<ElasticSearch::Transport.:HTTP>.


=head1 SYNOPSIS

    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'httplite',
        timeout     => '10',
    );

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport>

=item * L<ElasticSearch::Transport::HTTP>

=item * L<ElasticSearch::Transport::HTTPTiny>

=item * L<ElasticSearch::Transport::Curl>

=item * L<ElasticSearch::Transport::AEHTTP>

=item * L<ElasticSearch::Transport::AECurl>

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
