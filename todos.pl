#!/usr/bin/env perl
use strict;
use warnings;
use File::Find;

my $root = $ARGV[0] // '.';
my @files;

find(sub {
    $File::Find::prune = 1 if -d && /^(_build|\.git)$/;
    push @files, $File::Find::name if /\.ml(i|l)?$/ && -f;
}, $root);

my $count = 0;
for my $file (sort @files) {
    open my $fh, '<', $file or die "Cannot open $file: $!";
    my $lineno = 0;
    while (<$fh>) {
        $lineno++;
        if (/\bTODO\b[:\s]*(.*)/) {
            my $msg = $1;
            $msg =~ s/\s*\*\)\s*$//;  # strip OCaml comment close
            $msg =~ s/^\s+|\s+$//g;
            printf "%-60s  line %-4d  %s\n", $file, $lineno, $msg;
            $count++;
        }
    }
    close $fh;
}

print "\n$count TODO(s) found.\n";
