namespace triaxis.Data.SqlBuilder.ContextStates;

/// <summary>Base class for all <see cref="IStateContext{T}" /> states</summary>
public class Any { }

/// <summary>Base class for states where a WHERE condition may appear</summary>
public class Condition : Any { }

/// <summary>State following the SELECT statement</summary>
public class Select : Condition { }

/// <summary>State following the INSERT statement</summary>
public class Insert : Any { }

/// <summary>State following the INSERT statement where VALUES can no longer be added</summary>
public class PostInsert : Any { }

/// <summary>State following the UPDATE statement</summary>
public class Update : Condition { }

/// <summary>State in the middle of a SET clause, before value</summary>
public class Set : Condition { }

/// <summary>State following the DELETE statement</summary>
public class Delete : Condition { }

/// <summary>State in the middle of a WHERE condition, before operator and value</summary>
public class Where : Any { }
