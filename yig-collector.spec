Name:           yig-collector
Version:        1.0.0
Release:        1%{?dist}
Summary:        Push user access log to specified bucket

#Group:
License:        GPL
URL:            https://github.com/imegao/yig-collector
Source0:        yig-collector-1.0.0.tar.gz

#BuildRequires:
#Requires:

%description


%prep
%setup -q -n yig-collector


%build
go build


%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/usr/bin
install -D -m 755  yig-collector %{buildroot}/usr/bin
install -D -m 644  yig.service   %{buildroot}/usr/lib/systemd/system/yig-collector.service
install -D -m 644  config/yig-collector.toml   %{buildroot}/etc/yig/yig-collector.toml
%post
%clean
rm -rf %{buildroot}
%files
/usr/bin/yig-collector
/usr/lib/systemd/system/yig-collector.service
/etc/yig/yig-collector.toml
%doc
%defattr(-,root,root,-)

%changelog
%define __debug_install_post   \
   %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} "%{_builddir}/%{?buildsubdir}"\
%{nil}