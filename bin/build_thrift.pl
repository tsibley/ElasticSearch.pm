#!/usr/bin/env perl

use strict;
use warnings;
use Path::Class qw(dir file);
use LWP::Simple qw(getstore);
use File::Temp();
use Archive::Tar();
use File::Copy qw(copy);

my $cwd              = Cwd::cwd();
my @Thrift_Namespace = qw(ElasticSearch Transport ThriftBackend);
my $install_dir      = dir( $cwd, 'lib', @Thrift_Namespace );
my $Thrift_File_URL
    = 'http://github.com/elasticsearch/elasticsearch/raw/master/plugins/transport/thrift/elasticsearch.thrift';
my $Thrift_URL
    = 'http://apache.rediris.es/incubator/thrift/0.4.0-incubating/thrift-0.4.0.tar.gz';

my $temp_dir = File::Temp->newdir();
chdir $temp_dir or die $!;

unless ( -d $install_dir ) {
    $install_dir->mkpath || die "Couldn't create install dir: $!";
}

my $thrift_file = fetch_thrift_file();
print install_thrift($thrift_file);
sleep 10;

# fetch thrift file
# download thrift
# compile locally
# generate thrift code
# rename classes
# move Thrift module
# rename classes
#cleanup

#===================================
sub fetch_thrift_file {
#===================================
    my $thrift_file = fetch_file( $Thrift_File_URL, 'elasticsearch.thrift' );
    my $namespace = join( '.', @Thrift_Namespace );
    patch_file( $thrift_file, "/perl .*/perl $namespace/" );
    return $thrift_file;
}

#===================================
sub install_thrift {
#===================================
    my $thrift_file = shift;

    my $thrift_archive = fetch_file( $Thrift_URL, 'thrift.tar.gz' );
    print "Extracting thrift archive\n";
    my $archive = Archive::Tar->new();
    $archive->read("$thrift_archive");
    $archive->extract();

    my ($unzip_dir) = grep {-d} <thrift*>;
    my $build_dir = dir( $temp_dir, 'build' );
    $build_dir->mkpath;

    print "Compiling thrift\n";

    chdir $unzip_dir or die $!;
    $ENV{"${_}_PREFIX"} = "$build_dir" for qw(PY JAVA RUBY PHP PERL);
    system( './configure', '--prefix=' . $build_dir ) == 0
        or die "Couldn't configure thrift";
    system( 'make', 'install' ) == 0 or die "Couldn't make install  thrift";

    my $replace
        = "/(?<!:)"
        . "(Thrift|TApplicationException|TMessageType|TProtocolException"
        . "|TProtocolFactory|TTransportException|TType)/"
        . join( '::', @Thrift_Namespace, '' ) . '$1/g';

    my $source_dir = dir( $build_dir, 'lib', 'perl5' );

    my $dest_dir = $install_dir;

    print "Moving Thrift modules to $dest_dir\n";

    patch_file( file( $source_dir, 'Thrift.pm' ),
        $replace, file( $dest_dir, 'Thrift.pm' ) );
    $dest_dir = $dest_dir->subdir('Thrift');
    $dest_dir->mkpath();
    $source_dir = $source_dir->subdir('Thrift');
    while ( my $file = $source_dir->next ) {
        next unless -f $file;
        patch_file( $file, $replace, file( $dest_dir, $file->basename ) );
    }

    print "Generating thrift bindings\n";
    chdir $build_dir or die $!;

    system( 'bin/thrift', '--gen', 'perl', $thrift_file ) == 0
        or die "Couldn't generate Perl bindings";

    $dest_dir = $dest_dir->parent;
    print "Moving bindings to $dest_dir\n";

    my $perl_dir = dir( $build_dir, 'gen-perl', @Thrift_Namespace );
    while ( my $file = $perl_dir->next ) {
        next unless -f $file;
        patch_file( $file, $replace, file( $dest_dir, $file->basename ) );
    }
    chdir $cwd;
}

#===================================
sub fetch_file {
#===================================
    my $url  = shift;
    my $file = shift;

    print "Fetching file: $file\n";

    my $path = file( $temp_dir, $file );
    getstore( $url, "$path" ) == 200
        or die "Unable to retrieve file '$file'";
    return $path;
}
#===================================
sub patch_file {
#===================================
    my $file  = shift;
    my $regex = shift;
    my $dest  = shift || $file;
    local $/;
    open my $fh, '<', $file or die $!;
    my $content = <$fh>;
    eval "\$content=~s$regex" or die "Couldn't run regex on file '$file'";
    open $fh, '>', $dest or die $!;
    print $fh $content or die $!;
    close $fh or die $!;
}
