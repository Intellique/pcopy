#! /usr/bin/perl

use strict;
use warnings;

use Digest::SHA;
use File::Sync qw(fsync);
use Getopt::Long;
use locale;
use POSIX qw(locale_h strftime);
use Sys::CPU q/cpu_count/;

Getopt::Long::Configure('bundling');
setlocale( LC_MESSAGES, 'C' );

my $checksum_file = undef;
my $sparse_file   = 0;

my $nb_cpus    = cpu_count();
my $nb_jobs    = 0;
my $i_jobs     = 0;
my $chck_file  = undef;
my $log_file   = undef;
my $log_stderr = 0;

GetOptions(
    'checksum-file=s' => \$checksum_file,
    'help|h'          => \&print_help,
    'log-file=s'      => \$log_file,
    'log-stderr'      => \$log_stderr,
    'sparse'          => \$sparse_file,
);

my $output_dir = pop @ARGV;
my @input_dirs = @ARGV;

unless ( defined $output_dir ) {
    print_help();
}

my $log_fd = $log_stderr ? \*STDERR : \*STDOUT;
if ( defined $log_file ) {
    open( $log_fd, '>>', $log_file )
        or die "Can't open \"$log_file\" because $!";
}

if ( defined $checksum_file ) {
    open( $chck_file, '>', $checksum_file )
        or die "Can't open \"$checksum_file\" because $!";
}

sub message {
    my ( $job, $message ) = @_;
    printf {$log_fd} "#%03d: %s $message\n", $job, strftime( '%T', localtime );
}

sub print_help {
    print "Copy files and compare checksums\n";
    print "Usage: pcopy [options] <src-files...> <dest-files>\n";
    print "    --checksum-file <file>     : defer checksum computation after copy and write checksum into <file>\n";
    print "    --help, -h                 : Show this and exit\n";
    print "    --log-file <file>          : log into <file> instead of STDOUT\n";
    print "    --log-stderr               : log into STDERR instead of STDOUT\n";
    print "    --sparse                   : support for sparse file (experimental)\n";
    exit;
}

sub process {
    my ( $file, $input_dir ) = @_;

    my @info = lstat $file;

    if ( -l $file ) {
        my $new_file = $output_dir . '/' . substr( $file, rindex($input_dir, '/') + 1 );

        my $link;
        unless ( $link = readlink $file ) {
            message $i_jobs, "! error fatal, failed to read link of '$file' because $!";
            return 1;
        }

        message $i_jobs, "➜ create symbolic link from '$file' to '$new_file'";

        unless ( symlink $link, $new_file ) {
            message $i_jobs, "! error fatal, failed to create link of '$new_file' because $!";
            return 2;
        }
    }
    elsif ( -b $file ) {
        my $new_file = $output_dir . '/' . substr( $file, rindex($input_dir, '/') + 1 );

        message $i_jobs, "~ create block device '$file' to '$new_file'";

        my $major = $info[6] >> 8;
        my $minor = $info[6] & 0xFF;

        system 'mknod', $new_file, 'b', $major, $minor;

        message $i_jobs, "! error, failed to create block device '$file' to '$new_file'" if $? > 0;
    }
    elsif ( -c $file ) {
        my $new_file = $output_dir . '/' . substr( $file, rindex($input_dir, '/') + 1 );

        message $i_jobs, "~ create character device '$file' to '$new_file'";

        my $major = $info[6] >> 8;
        my $minor = $info[6] & 0xFF;

        system 'mknod', $new_file, 'c', $major, $minor;

        message $i_jobs, "! error, failed to create character device '$file' to '$new_file'" if $? > 0;
    }
    elsif ( -f $file ) {
        $i_jobs++;

        my $pid = fork();
        unless ( defined $pid ) {
            message $i_jobs, "! error fatal, failed to fork process because $!";
            return 1;
        }

        if ( $pid == 0 ) {
            my $new_file = $output_dir . '/' . substr( $file, rindex($input_dir, '/') + 1 );

            message $i_jobs, "@ copy '$file' to '$new_file'";

            my ( $file_in, $file_out );
            unless ( open $file_in, '< :raw', $file ) {
                message $i_jobs, "! error while opening '$file' because $!";
                exit 2;
            }

            my $data_pos = 0;
            my $hole_pos = 512 * $info[12];

            if ($sparse_file) {
                message( $i_jobs, "⊆ '$new_file' is a sparse file" ) if ( $info[7] > 512 * $info[12] );

                # find start of data
                seek $file_in, 0, 3;
                $data_pos = tell $file_in;

                # find start of hole
                seek $file_in, 0, 4;
                $hole_pos = tell $file_in;

                # reset position
                seek $file_in, 0, 0;
            }

            unless ( open $file_out, '> :raw', $new_file ) {
                message $i_jobs, "! error while opening '$new_file' because $!";
                exit 3;
            }

            my $digest = Digest::SHA->new('1');

            my $length = $data_pos < $hole_pos ? $hole_pos : $data_pos;
            $length = 65536 if $length > 65536;

            my $pos = 0;
            while ( my $nb_read = sysread $file_in, my $buffer, $length ) {
                if ( $data_pos < $hole_pos ) {
                    unless ( syswrite $file_out, $buffer ) {
                        message $i_jobs, "! error while writing into '$new_file' because $!";

                        close $file_in;
                        close $file_out;
                        unlink $new_file;

                        exit 4;
                    }
                }
                else {
                    unless ( truncate $file_out, $data_pos ) {
                        message $i_jobs, "! error while seeking into '$new_file' because $!";

                        close $file_in;
                        close $file_out;
                        unlink $new_file;

                        exit 4;
                    }
                }

                $digest->add($buffer);

                $pos += $nb_read;

                if ($sparse_file) {
                    if ( $data_pos == $pos ) {
                        seek $file_in, $pos, 4;
                        $hole_pos = tell $file_in;
                        seek $file_in, $pos, 0;
                    }
                    elsif ( $hole_pos == $pos ) {
                        seek $file_in, $pos, 3;
                        $data_pos = tell $file_in;
                        seek $file_in, $pos, 0;

                        $data_pos = $info[7] if $data_pos == 0;
                    }
                }

                $length = $data_pos < $hole_pos ? $hole_pos : $data_pos;
                $length = 65536 if $length > 65536;
            }

            if ($!) {
                message $i_jobs, "! error while reading from '$file' because $!";

                close $file_in;
                close $file_out;
                unlink $new_file;

                exit 5;
            }

            if ( $data_pos == $hole_pos and $pos == 0 ) {
                unless ( truncate $file_out, $info[7] ) {
                    message $i_jobs, "! warning while truncate '$new_file' to size ${info[7]} because $!";
                }
            }

            message $i_jobs, "> flush file '$new_file'";

            unless ( fsync $file_out) {
                message $i_jobs, "! error while fsyncing from '$file' because $!";

                close $file_in;
                close $file_out;
                unlink $new_file;

                exit 6;
            }

            close $file_in;
            close $file_out;
            my $src_digest = $digest->hexdigest;

            if ( defined $chck_file ) {
                print {$chck_file} "$src_digest  $new_file\n";

                my @stat = stat $file;
                unless ( chmod $stat[2] & 0777, $new_file ) {
                    message $i_jobs, "! warning while changing owner of '$new_file' because $!";
                }

                unless ( chown $stat[4], $stat[5], $new_file ) {
                    message $i_jobs, "! warning while changing permission of '$new_file' because $!";
                }

                unless ( utime $stat[8], $stat[9], $new_file) {
                    message $i_jobs, "! warning while changing access and modification time of '$new_file' because $!";
                }

                exit;
            }

            message $i_jobs, "# compute sha1 of '$new_file'";

            unless ( open $file_in, '< :raw', $new_file ) {
                message $i_jobs, "! error while opening '$new_file' because $!";
                exit 7;
            }

            $digest = Digest::SHA->new('1');

            while ( sysread $file_in, my $buffer, 65536 ) {
                $digest->add($buffer);
            }

            if ($!) {
                message $i_jobs, "! error while reading from '$new_file' because $!";

                close $file_in;
                unlink $new_file;

                exit 8;
            }

            close $file_in;
            my $dest_digest = $digest->hexdigest;

            if ( $src_digest eq $dest_digest ) {
                message $i_jobs, "= digests match (digest: $src_digest) between '$file' and '$new_file'";

                my @stat = stat $file;
                unless ( chmod $stat[2] & 0777, $new_file ) {
                    message $i_jobs, "! warning while changing owner of '$new_file' because $!";
                }

                unless ( chown $stat[4], $stat[5], $new_file ) {
                    message $i_jobs, "! warning while changing permission of '$new_file' because $!";
                }

                unless ( utime $stat[8], $stat[9], $new_file) {
                    message $i_jobs, "! warning while changing access and modification time of '$new_file' because $!";
                }

                exit;
            }
            else {
                message $i_jobs, "≠ digests mismatch between '$file'[$src_digest] and '$new_file'[$dest_digest]";
                unlink $new_file;
            }

            exit;
        }

        $nb_jobs++;
        if ( $nb_jobs >= $nb_cpus ) {
            wait;
            $nb_jobs--;
        }
    }
    elsif ( -d $file ) {
        my $new_dir = $output_dir . '/' . substr( $file, rindex($input_dir, '/') + 1 );
        $new_dir =~ s;^(.*[^/])/*$;$1;;

        unless ( -d $new_dir ) {
            $i_jobs++;

            message $i_jobs, "~ mkdir '$new_dir'";

            my @stat = stat $file;

            unless ( mkdir $new_dir, $stat[2] & 0777 ) {
                message $i_jobs, "! error while creating directory '$new_dir' because $!";
                return 1;
            }

            unless ( chown $stat[4], $stat[5], $new_dir ) {
                message $i_jobs, "! warning while changing owner of '$new_dir' because $!";
            }
        }

        my $dir;
        opendir $dir, $file;
        my @files = sort grep { not /^\.{1,2}$/ } readdir $dir;
        closedir $dir;

        $file =~ s;^(.*[^/])/*$;$1;;
        foreach my $sub_file (@files) {
            process( "$file/$sub_file", $input_dir ) and last;
        }
    }

    return 0;
}

foreach my $input_dir (@input_dirs) {
    process( $input_dir, $input_dir );
}

while ( $nb_jobs > 0 ) {
    wait;
    $nb_jobs--;
}

exit unless defined $chck_file;

close $chck_file;
open $chck_file, '<', $checksum_file
    or die "Can't open \"$checksum_file\" because $!";

while (<$chck_file>) {
    if (/^(\w+)  (.*)$/) {
        my ( $src_digest, $file ) = ( $1, $2 );
        $i_jobs++;

        my $pid = fork();
        unless ( defined $pid ) {
            message $i_jobs, "! error fatal, failed to fork process because $!";
            last;
        }

        if ( $pid == 0 ) {
            message $i_jobs, "# compute sha1 of '$file'";

            my $file_in;
            unless ( open $file_in, '< :raw', $file ) {
                message $i_jobs, "! error while opening '$file' because $!";
                exit 7;
            }

            my $digest = Digest::SHA->new('1');

            while ( sysread $file_in, my $buffer, 65536 ) {
                $digest->add($buffer);
            }

            if ($!) {
                message $i_jobs, "! error while reading from '$file' because $!";

                close $file_in;
                exit 8;
            }

            close $file_in;
            my $dest_digest = $digest->hexdigest;

            if ( $src_digest eq $dest_digest ) {
                message $i_jobs, "= digests match (digest: $src_digest) '$file'";
            }
            else {
                message $i_jobs, "≠ digests mismatch between 'src'[$src_digest] and '$file'[$dest_digest]";
            }

            exit;
        }

        $nb_jobs++;
        if ( $nb_jobs >= $nb_cpus ) {
            wait;
            $nb_jobs--;
        }
    }
}

close $chck_file;

while ( $nb_jobs > 0 ) {
    wait;
    $nb_jobs--;
}

