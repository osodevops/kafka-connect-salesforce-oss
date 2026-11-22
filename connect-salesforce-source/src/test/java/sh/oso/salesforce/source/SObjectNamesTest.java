package sh.oso.salesforce.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class SObjectNamesTest {

    @Test
    void standardObjectIsUnchanged() {
        assertThat(SObjectNames.apiName("Account")).isEqualTo("Account");
        assertThat(SObjectNames.cdcChannel("Account")).isEqualTo("/data/AccountChangeEvent");
    }

    @Test
    void customObjectByChangeEventEntityName() {
        assertThat(SObjectNames.apiName("My_Object__")).isEqualTo("My_Object__c");
        assertThat(SObjectNames.cdcChannel("My_Object__")).isEqualTo("/data/My_Object__ChangeEvent");
    }

    @Test
    void customObjectByApiName() {
        assertThat(SObjectNames.apiName("My_Object__c")).isEqualTo("My_Object__c");
        assertThat(SObjectNames.cdcChannel("My_Object__c")).isEqualTo("/data/My_Object__ChangeEvent");
    }

    @Test
    void namespacedCustomObject() {
        assertThat(SObjectNames.apiName("ns__My_Object__")).isEqualTo("ns__My_Object__c");
        assertThat(SObjectNames.cdcChannel("ns__My_Object__c")).isEqualTo("/data/ns__My_Object__ChangeEvent");
    }

    @Test
    void namespacedCustomObjectResolvesTheSameFromEitherSpelling() {
        assertThat(SObjectNames.apiName("ns__My_Object__c")).isEqualTo("ns__My_Object__c");
        assertThat(SObjectNames.cdcChannel("ns__My_Object__")).isEqualTo("/data/ns__My_Object__ChangeEvent");
    }

    /**
     * Only the custom object suffix is rewritten. Other suffixed types keep their API name for
     * describe and queries (polling mode); Salesforce publishes no change events for external
     * objects, big objects or custom metadata types.
     */
    @ParameterizedTest
    @ValueSource(strings = {"Knowledge__kav", "Order_Line__x", "Audit__b", "Setting__mdt", "My_Object__Share"})
    void otherSuffixesKeepTheirApiName(String configured) {
        assertThat(SObjectNames.apiName(configured)).isEqualTo(configured);
        assertThat(SObjectNames.cdcChannel(configured)).isEqualTo("/data/" + configured + "ChangeEvent");
    }

    @Test
    void suffixMatchIsCaseSensitiveLikeTheConfiguredName() {
        // sf.sobjects is documented as exact casing; an upper-case C is not treated as the suffix.
        assertThat(SObjectNames.cdcChannel("My_Object__C")).isEqualTo("/data/My_Object__CChangeEvent");
    }
}
