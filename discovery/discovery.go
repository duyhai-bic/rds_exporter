package discovery

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
)

// GetRDSInstanceIdentifiers retrieves all RDS instance identifiers using an AWS session.
func getRDSInstanceIdentifiers(sess *session.Session) ([]string, error) {
	// Create a new RDS service client from the session.
	svc := rds.New(sess)

	var identifiers []string
	input := &rds.DescribeDBInstancesInput{}

	// Use DescribeDBInstancesPages to handle pagination.
	err := svc.DescribeDBInstancesPages(input,
		func(page *rds.DescribeDBInstancesOutput, lastPage bool) bool {
			for _, instance := range page.DBInstances {
				if instance.DBInstanceIdentifier != nil {
					identifiers = append(identifiers, *instance.DBInstanceIdentifier)
				}
			}
			// Return true to keep paging.
			return true
		})
	if err != nil {
		return nil, err
	}
	return identifiers, nil
}

func New(sess *session.Session) ([]string, error) {
	// Initial immediate discovery
	return getRDSInstanceIdentifiers(sess)
}
