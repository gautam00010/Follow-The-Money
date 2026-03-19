import textwrap
import unittest
from decimal import Decimal

from scripts.fetch_insider_purchases import InsiderPurchase, parse_purchase_transactions


class ParsePurchaseTransactionsTests(unittest.TestCase):
    def test_parse_purchase_transactions_filters_by_value(self) -> None:
        sample_xml = textwrap.dedent(
            """
            <ownershipDocument>
              <issuer>
                <issuerName>Sample Corp</issuerName>
                <issuerTradingSymbol>SAMP</issuerTradingSymbol>
              </issuer>
              <reportingOwner>
                <reportingOwnerId>
                  <rptOwnerName>Jane Doe</rptOwnerName>
                </reportingOwnerId>
              </reportingOwner>
              <nonDerivativeTable>
                <nonDerivativeTransaction>
                  <transactionDate><value>2024-03-01</value></transactionDate>
                  <transactionCoding>
                    <transactionCode>P</transactionCode>
                  </transactionCoding>
                  <transactionAmounts>
                    <transactionShares><value>2000</value></transactionShares>
                    <transactionPricePerShare><value>30</value></transactionPricePerShare>
                  </transactionAmounts>
                </nonDerivativeTransaction>
                <nonDerivativeTransaction>
                  <transactionDate><value>2024-03-02</value></transactionDate>
                  <transactionCoding>
                    <transactionCode>S</transactionCode>
                  </transactionCoding>
                  <transactionAmounts>
                    <transactionShares><value>1000</value></transactionShares>
                    <transactionPricePerShare><value>20</value></transactionPricePerShare>
                  </transactionAmounts>
                </nonDerivativeTransaction>
              </nonDerivativeTable>
            </ownershipDocument>
            """
        )

        purchases = parse_purchase_transactions(sample_xml, minimum_value=Decimal("50000"))

        self.assertEqual(len(purchases), 1)
        purchase = purchases[0]
        self.assertIsInstance(purchase, InsiderPurchase)
        self.assertEqual(purchase.issuer_name, "Sample Corp")
        self.assertEqual(purchase.ticker, "SAMP")
        self.assertEqual(purchase.insider, "Jane Doe")
        self.assertEqual(purchase.date, "2024-03-01")
        self.assertEqual(purchase.shares, Decimal("2000"))
        self.assertEqual(purchase.price, Decimal("30"))
        self.assertEqual(purchase.value, Decimal("60000"))


if __name__ == "__main__":
    unittest.main()
