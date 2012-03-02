package ElasticSearch::TestServer;

use strict;
use warnings;
use ElasticSearch();
use POSIX 'setsid';
use IO::Socket();
use File::Temp 0.22 ();
use File::Spec::Functions qw(catfile);
use YAML qw(DumpFile);
use File::Path qw(rmtree);

use base 'ElasticSearch';

=head1 NAME

ElasticSearch::TestServer - Start an ElasticSearch cluster for testing

=head1 SYNOPSIS

    use ElasticSearch::TestServer;

    $ENV{ES_HOME} = '/path/to/elasticsearch';
    $ENV{ES_TRANSPORT} = 'http';

    my $es = ElasticSearch::TestServer->new(
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
the C<transport> is C<http>, C<httplite>, C<httptiny>, C<curl>, C<aehttp>
 or 9500 if C<thrift>.

It is a subclass of L<ElasticSearch>, so C<< ElasticSearch::TestServer->new >>
returns an ElasticSearch instance.

=cut

#===================================
sub new {
#===================================
    my $class  = shift;
    my %params = (
        home      => $ENV{ES_HOME},
        transport => $ENV{ES_TRANSPORT} || 'http',
        instances => 3,
        ip        => '127.0.0.1',
        ref $_[0] eq 'HASH' ? %{ shift() } : @_
    );

    my $home = delete $params{home} or die <<NO_HOME;

************************************************************
    ElasticSearch home directory not specified

    Please either set \$ENV{ES_HOME} or pass a value
    for 'home' to new()

************************************************************

NO_HOME

    my $transport = $params{transport};
    my $port      = delete $params{port}
        || ( $transport eq 'thrift' ? 9500 : 9200 );
    my $instances = delete $params{instances};
    my $plugin    = $ElasticSearch::Transport::Transport{$transport}
        or die "Unknown transport '$transport'";
    eval "require  $plugin" or die $@;
    $plugin->_make_sync if $plugin->can('_make_sync');
    my $protocol = $plugin->protocol;

    my %config = (
        cluster => { name => 'es_test' },
        gateway => { type => 'local' },
        "$protocol.port" => "$port-" . ( $port + $instances - 1 ),
        %{ $params{config} || {} }
    );

    my $ip = $config{network}{host} = delete $params{ip};
    my @servers = map {"$ip:$_"} $port .. $port + $instances - 1;

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

    my $cmd          = catfile( $home, 'bin', 'elasticsearch' );
    my $pid_file     = File::Temp->new;
    my $blank_config = File::Temp->new( SUFFIX => '.yml' );
    my $config_path  = $blank_config->filename();

    my $dir     = '';
    my $dirname = '';
    my $PIDs    = [];

    unless ( $config{path}{data} ) {
        $dir = File::Temp->newdir(
            'elastic_XXXXX',
            CLEANUP => 0,
            TMPDIR  => 1
        );
        $dirname = $config{path}{data} = $dir->dirname;
    }

    my $old_SIGINT = $SIG{INT};
    my $new_SIGINT = sub {
        $class->_shutdown_servers( $PIDs, $dirname );
        if ( ref $old_SIGINT eq 'CODE' ) {
            return $old_SIGINT->();
        }
        exit(1);
    };
    $SIG{INT} = $new_SIGINT;

    DumpFile( $blank_config->filename, \%config );

    for ( 1 .. $instances ) {
        print "Starting test node $_\n";
        my $int_caught = 0;
        local $SIG{INT} = sub { $int_caught++; };
        defined( my $pid = fork ) or die "Couldn't fork a new process: $!";
        if ( $pid == 0 ) {
            die "Can't start a new session: $!" if setsid == -1;
            exec( $cmd, '-p', $pid_file->filename,
                '-Des.config=' . $config_path );
        }
        else {
            sleep 1;
            open my $pid_fh, '<', $pid_file->filename;
            my $pid = <$pid_fh>;
            die "ES is running, but no PID found" unless $pid;
            chomp $pid;
            push @$PIDs, $pid;
        }
        $new_SIGINT->() if $int_caught;
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
    if (@servers) {
        eval { $class->_shutdown_servers( $PIDs, $dirname ) };
        die "Couldn't start $instances nodes for transport $transport";
    }

    my $es = eval {
        $class->SUPER::new(
            %params,
            servers     => $server,
            trace_calls => $params{trace_calls},
            transport   => $transport,
            pids        => $PIDs,
            tmpdir      => $dirname,
        );
    };
    unless ($es) {
        my $error = $@;
        $class->_shutdown_servers( $PIDs, $dirname );
        die $error;
    }

    my $attempts = 10;
    while (1) {
        eval { $es->refresh_servers; 1 } && last;
        die("**** Couldn't connect to ElasticSearch at $server ****\n")
            unless --$attempts;
        print "Connection failed. Retrying\n";
        sleep 1;
    }
    print "Connected\n";

    return $es;
}

#===================================
sub pids {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_pids} = shift;
    }
    return $self->{_pids};
}

#===================================
sub tmpdir {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_tmpdir} = shift;
    }
    return $self->{_tmpdir};
}

#===================================
sub _shutdown_servers {
#===================================
    my ( $self, $PIDs, $dir ) = @_;

    local $?;

    $PIDs = $self->pids   unless defined $PIDs;
    $dir  = $self->tmpdir unless defined $dir;

    return unless $PIDs;

    kill 9, @$PIDs;
    sleep 1;

    while (1) { last if wait == -1 }
    if ( defined $dir ) {
        rmtree( $dir, { error => \my $error } );
    }
    undef $dir;
}

sub DESTROY { shift->_shutdown_servers; }

=head1 AUTHOR

Clinton Gormley, E<lt>clinton@traveljury.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Clinton Gormley

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut

1
