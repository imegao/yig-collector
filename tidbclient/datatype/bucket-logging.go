package datatype

import "encoding/xml"

type BucketLoggingStatus struct {
	XMLName xml.Name        `xml:"BucketLoggingStatus"`
	LoggingEnabled    BucketLoggingRule `xml:"LoggingEnabled"`
}

type BucketLoggingRule struct {
	TargetBucket     string `xml:"TargetBucket"`
	TargetPrefix     string `xml:"TargetPrefix"`
	TargetGrants     TargetGrant `xml:"TargetGrants"`
}

type TargetGrant struct {
	XMLName    xml.Name `xml:"TargetGrant"`
	Grants [] Grant `xml:"Grant"`
}

type Grant struct {
	XMLName    xml.Name `xml:"Grant"`
	Grantee Grantee `xml:"Grantee"`
	Permission string    `xml:"Permission"`
}
type Grantee struct {
	XMLName      xml.Name `xml:"Grantee"`
	DisplayName string `xml:"DisplayName,omitempty"`
	EmailAddress string `xml:"EmailAddress,omitempty"`
	ID string `xml:"ID,omitempty"`
	XsiType string `xml:"xsi:type,attr"`
	URI  string `xml:"URI,omitempty"`
}
