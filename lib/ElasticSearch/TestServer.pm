package ElasticSearch::TestServer;

use strict;
use warnings;
use ElasticSearch();
use POSIX 'setsid';
use IO::Socket();
use File::Temp();
use File::Spec::Functions qw(catfile);
use YAML qw(DumpFile);

require Exporter;
our @ISA    = 'Exporter';
our @EXPORT = qw(connect_to_es);

=head1 NAME

ElasticSearch::TestServer - Start an ElasticSearh cluster for testing

=head1 SYNOPSIS

    use ElasticSearch::TestServer;

    $ENV{ES_HOME} = '/path/to/elasticsearch';
    $ENV{ES_TRANSPORT} = 'http';

    my $es = connect_to_es();

  OR

    my $es = connect_to_es(
        home        => '/path/to/elasticsearch',
        instances   => 3,
        transport   => 'http',
        ip          => '127.0.0.1',
        trace_calls => 'logfile',
        port        => '9200',
        config      => { values to override}
    );

=head1 DESCRIPTION

ElasticSearch::TestServer is a utility module which will start an
ElasticSearch cluster intended for testing, and shut the cluster
down at the end, even if your code exits abnormally.

By default, it uses C<http> transport, the C<local> gateway, and
starts 3 instances on C<localhost>, starting with C<port> 9200 if
the C<transport> is C<http> or C<httplite>, or 9500 if C<thrift>.

C<connect_to_es> returns an ElasticSearch instance.

=cut

my ( @PIDs, $work_dir );

#===================================
sub connect_to_es {
#===================================
    my %params = (
        home      => $ENV{ES_HOME},
        transport => $ENV{ES_TRANSPORT} || 'http',
        instances => 3,
        ip        => '127.0.0.1',
        ref $_[0] eq 'HASH' ? %{ shift() } : @_
    );

    my $home = $params{home} or die <<NO_HOME;

************************************************************
    ElasticSearch home directory not specified

    Please either set \$ENV{ES_HOME} or pass a value
    for 'home' to connect_to_es()

************************************************************

NO_HOME

    my %config = (
        cluster => { name => 'es_test' },
        gateway => { type => 'local' },
        %{ $params{config} || {} }
    );
    my $transport = $params{transport};
    my $port      = $params{port} || ( $transport eq 'thrift' ? 9500 : 9200 );
    my $instances = $params{instances};
    my $ip        = $config{network}{host} = $params{ip};
    my @servers   = map {"$ip:$_"} $port .. $port + $instances - 1;

    foreach (@servers) {
        if ( IO::Socket::INET->new($_) ) {
            die <<RUNNING;

************************************************************

    There is already a server running on $_.
    Please shut it down before starting the test server

************************************************************
RUNNING
        }
    }

    my $server = $servers[0];

    print "Starting test server installed in $home\n";

    my $cmd = catfile( $home, 'bin', 'elasticsearch' );
    my $pid_file = File::Temp->new;

    my $blank_config = File::Temp->new( SUFFIX => '.yml' );
    my $config_path = $blank_config->filename();

    unless ( $config{path}{data} ) {
        $work_dir = File::Temp->newdir();
        $config{path}{data} = $work_dir->dirname;
    }

    DumpFile( $blank_config->filename, \%config );

    $SIG{INT} = sub { _shutdown_servers(); };

    for ( 1 .. $instances ) {
        print "Starting test node $_\n";
        defined( my $pid = fork ) or die "Couldn't fork a new process: $!";
        if ( $pid == 0 ) {
            die "Can't start a new session: $!" if setsid == -1;
            exec( $cmd, '-p', $pid_file->filename,
                '-Des.config=' . $config_path );
        }
        else {
            sleep 1;
            open my $pid_fh, '<', $pid_file->filename;
            push @PIDs, <$pid_fh>;
        }

    }

    print "Waiting for servers to warm up\n";

    my $timeout = 20;
    while (@servers) {
        if ( IO::Socket::INET->new( $servers[0] ) ) {
            print "Node running on $servers[0]\n";
            shift @servers;
        }
        else {
            sleep 1;
        }
        $timeout--;
        last if $timeout == 0;
    }
    die "Couldn't start $instances nodes" if @servers;

    my $es;
    eval {
        $es = ElasticSearch->new(
            servers     => $server,
            trace_calls => $params{trace_calls},
            transport   => $transport,
        );
        $es->refresh_servers;
    }
        or die("**** Couldn't connect to ElasticSearch at $server ****");
    return $es;
}

#===================================
sub _shutdown_servers {
#===================================
    kill 9, @PIDs;
    wait;
    exit(0);
}

END { _shutdown_servers() }

=head1 AUTHOR

Clinton Gormley, E<lt>clinton@traveljury.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Clinton Gormley

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut

1
