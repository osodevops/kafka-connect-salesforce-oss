package sh.oso.salesforce.source;

import org.junit.jupiter.api.Test;

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
}
