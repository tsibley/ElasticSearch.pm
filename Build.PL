use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name       => 'ElasticSearch',
    license           => 'perl',
    dist_author       => q{Clinton Gormley <drtech@cpan.org>},
    dist_version_from => 'lib/ElasticSearch.pm',
    build_requires    => {
        'Test::More'      => 0,
        'Test::Exception' => 0,
        'POSIX'           => 0,
        'IO::Socket'      => 0,
        'File::Temp'      => '0.20',
        'YAML'            => 0,
    },
    add_to_cleanup     => ['ElasticSearch-*'],
    create_makefile_pl => 'traditional',
    configure_requires => {
        'Module::Build' => 0.38,
    },
    requires           => {
        'LWP::UserAgent'               => 0,
        'LWP::ConnCache'               => 0,
        'HTTP::Request'                => 0,
        'JSON'                         => 0,
        'Data::Dumper'                 => 0,
        'Encode'                       => 0,
        'parent'                       => 0,
        'Scalar::Util'                 => '1.07',
        'ElasticSearch::SearchBuilder' => '0.06',
    },
    recommends => {
        'HTTP::Lite'      => 0,
        'HTTP::Tiny'      => 0,
        'Bit::Vector'     => 0,
        'Class::Accessor' => 0,
        'JSON::XS'        => 0,
    },
    meta_merge => {
        resources => {
            repository =>
                'http://github.com/clintongormley/ElasticSearch.pm/',
            bugtracker =>
                "https://github.com/clintongormley/ElasticSearch.pm/issues",
        }
    },
);

$builder->create_build_script();
