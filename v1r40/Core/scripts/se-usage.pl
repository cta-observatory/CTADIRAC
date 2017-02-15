#!/usr/bin/env perl

use Net::LDAP;

my @IGNORE_SES = qw(srmifae.pic.es srmcms.pic.es srmatlas.pic.es srmlhcb.pic.es styx.zeuthen.desy.de);

my $SE = shift();
my $VO = shift() || 'vo.cta.in2p3.fr';
my $ses = vo_ses();

#use Dumpvalue;
#my $d = new Dumpvalue;
#$d->dumpValue($ses);

my ($solu, $solf, $snlu, $snlf) = (0, 0, 0, 0);
printf "%18s %32s %8s %8s %8s %8s %s\n", 'SITE', 'SE', 'OL(used)', 'OL(free)', 'NL(used)', 'NL(free)', 'PATH';
print '-'x130, "\n";
foreach my $se ( sort { $ses->{$a}{'site'} cmp $ses->{$b}{'site'} } keys %$ses ) {
    next if $SE && $SE ne $se;
    next if grep /^$se$/, @IGNORE_SES;
    my $site = $ses->{$se}{'site'};
    while ( my ($path, $data) = each %{$ses->{$se}{'path'}} ) {
	next if $path !~ /^\//;
	printf "%18s %32s %8.1f %8.1f %8.1f %8.1f %s\n", $site, $se, $data->{'ol_used'}/1000, $data->{'ol_free'}/1000, $data->{'nl_used'}/1000, $data->{'nl_free'}/1000, $path;
	$solu += $data->{'ol_used'};
	$solf += $data->{'ol_free'};
	$snlu += $data->{'nl_used'};
	$snlf += $data->{'nl_free'};
    }
}
print '-'x130, "\n";
printf "%18s %32s %8.1f %8.1f %8.1f %8.1f\n", 'SUM:', '', $solu/1000, $solf/1000, $snlu/1000, $snlf/1000;

sub vo_ses {
    my $bdii = $ENV{'LCG_GFAL_INFOSYS'} || 'top-bdii.cern.ch:2170';
    $bdii =~ s/,.*//; # just in case it's a bdii list ...
    my ($bdii_host, $port) = split ':', $bdii;
    my $ldap = Net::LDAP->new($bdii_host, 'port' => $port, 'timeout' => 5)
	or die "Could not query bdii $bdii ($@)";
    $ldap->bind();
    my $ses = {};

    my $result = $ldap->search(
	'base'   => 'mds-vo-name=local,o=grid',
	'filter' => "(GlueVOInfoAccessControlBaseRule=VO:$VO)",
	'attrs'  => [ qw(GlueChunkKey GlueVOInfoPath) ]
    );
    foreach my $entry ( $result->entries() ) {
	my @chunks = $entry->get_value('GlueChunkKey');
	my $said = (grep(/^GlueSALocalID/, @chunks))[0];
	$said =~ s/^GlueSALocalID=//;
	my $se = (grep(/^GlueSEUniqueID/, @chunks))[0];
	$se =~ s/^GlueSEUniqueID=//;
	my $path = $entry->get_value('GlueVOInfoPath');
	$ses->{$se}{'store'}{$said}{'path'} = $path;
    }
    $result = $ldap->search(
	'base'   => 'mds-vo-name=local,o=grid',
	'filter' => "(GlueSAAccessControlBaseRule=VO:$VO)",
	'attrs'  => [ qw(GlueChunkKey GlueSALocalID GlueSAUsedOnlineSize GlueSAFreeOnlineSize GlueSAUsedNearlineSize GlueSAFreeNearlineSize) ]
    );
    foreach my $entry ( $result->entries() ) {
	my $se = $entry->get_value('GlueChunkKey');
	$se =~ s/^GlueSEUniqueID=//;
	$said = $entry->get_value('GlueSALocalID');
	$path = $ses->{$se}{'store'}{$said}{'path'} || $said;
	$ses->{$se}{'path'}{$path}{'ol_used'} += $entry->get_value('GlueSAUsedOnlineSize');
	$ses->{$se}{'path'}{$path}{'ol_free'} += $entry->get_value('GlueSAFreeOnlineSize');
	$ses->{$se}{'path'}{$path}{'nl_used'} += $entry->get_value('GlueSAUsedNearlineSize');
	$ses->{$se}{'path'}{$path}{'nl_free'} += $entry->get_value('GlueSAFreeNearlineSize');
    }

    my $filter = ''; my $sites = {};
    $filter .= "(GlueSEUniqueID=$_)" foreach keys %{$ses};
    my $result = $ldap->search(
	'base'   => 'mds-vo-name=local,o=grid',
	'filter' => "(|$filter)",
	'attrs'  => [ qw(GlueSEUniqueID GlueSEImplementationName GlueForeignKey) ]
    );
    foreach my $entry ( $result->entries() ) {
	my $site = $entry->get_value('GlueForeignKey');
	$site =~ s/^GlueSiteUniqueID=//;
	$se = $entry->get_value('GlueSEUniqueID');
	$ses->{$se}{'site'} = $site;
	$ses->{$se}{'implementation'} = $entry->get_value('GlueSEImplementationName');
    }

    $ldap->unbind();
    return $ses;
}
