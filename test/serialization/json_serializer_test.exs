defmodule Commanded.Serialization.JsonSerializerTest do
	use ExUnit.Case
	doctest Commanded.Serialization.JsonSerializer

  alias Commanded.Serialization.JsonSerializer
  alias Commanded.ExampleDomain.BankAccount.Events.{BankAccountOpened}

  @serialized_event_json "{\"initial_balance\":1000,\"account_number\":\"ACC123\"}"
  
	test "should serialize event to JSON" do
    account_opened = %BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}

    assert JsonSerializer.serialize(account_opened) == @serialized_event_json
  end

  test "should deserialize event from JSON" do
    account_opened = %BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}
    type = Atom.to_string(account_opened.__struct__)

    assert JsonSerializer.deserialize(@serialized_event_json, type: type) == account_opened
  end
end
